package io.digdag.standards.operator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
import io.digdag.core.Environment;
import io.digdag.core.Version;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.standards.Proxies;
import io.digdag.standards.operator.state.PollingRetryExecutor;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.BasicAuthentication;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.eclipse.jetty.http.HttpHeader.AUTHORIZATION;
import static org.eclipse.jetty.http.HttpHeader.USER_AGENT;

public class HttpOperatorFactory
        implements OperatorFactory
{
    private static final Logger logger = LoggerFactory.getLogger(HttpOperatorFactory.class);

    private final Optional<ProxyConfiguration.Proxy> systemProxy;
    private final Map<String, String> env;
    private final boolean allowUserProxy;
    private final int maxRedirects;
    private final String userAgent;

    @Inject
    public HttpOperatorFactory(Config systemConfig, @Environment Map<String, String> env)
    {
        this.allowUserProxy = systemConfig.get("config.http.allow_user_proxy", boolean.class, true);
        this.systemProxy = systemProxy(systemConfig);
        this.maxRedirects = systemConfig.get("config.http.max_redirects", int.class, 8);
        this.env = env;
        this.userAgent = systemConfig.get("config.http.user_agent", String.class, "Digdag/" + Version.buildVersion());
    }

    private static Optional<ProxyConfiguration.Proxy> systemProxy(Config systemConfig)
    {
        boolean enabled = systemConfig.get("config.http.proxy.enabled", boolean.class, false);
        if (!enabled) {
            return Optional.absent();
        }
        String host = systemConfig.get("config.http.proxy.host", String.class);
        int port = systemConfig.get("config.http.proxy.port", int.class);
        boolean tls = systemConfig.get("config.http.proxy.tls", boolean.class, false);
        HttpProxy proxy = new HttpProxy(new Origin.Address(host, port), tls);
        return Optional.of(proxy);
    }

    @Override
    public String getType()
    {
        return "http";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new HttpOperator(projectPath, request);
    }

    private class HttpOperator
            extends BaseOperator
    {
        private final TaskState state;
        private final Config params;

        private HttpOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request);
            this.state = TaskState.of(request);
            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("http"));
        }

        @Override
        public List<String> secretSelectors()
        {
            return ImmutableList.of("http.*");
        }

        @Override
        public TaskResult runTask(TaskExecutionContext ctx)
        {
            HttpClient client = client();
            try {
                return run(ctx, client);
            }
            finally {
                stop(client);
            }
        }

        private TaskResult run(TaskExecutionContext ctx, HttpClient httpClient)
        {
            // TODO: support secrets in headers, uri fragments, payload fragments, etc

            SecretProvider httpSecrets = ctx.secrets().getSecrets("http");

            URI uri = URI.create(httpSecrets.getSecretOptional("uri").or(() -> params.get("_command", String.class)));
            String method = params.get("method", String.class, "GET");

            Optional<String> user = httpSecrets.getSecretOptional("user");
            Optional<String> authorization = httpSecrets.getSecretOptional("authorization");

            Request request = httpClient.newRequest(uri)
                    .method(method)
                    .timeout(30, SECONDS);

            if (authorization.isPresent()) {
                request.header(AUTHORIZATION, authorization.get());
            }
            else if (user.isPresent()) {
                Optional<String> password = httpSecrets.getSecretOptional("password");
                httpClient.getAuthenticationStore().addAuthenticationResult(new BasicAuthentication.BasicResult(uri, user.get(), password.or("")));
            }

            Optional<String> content = params.getOptional("content", String.class);
            Optional<String> contentType = params.getOptional("content_type", String.class);

            Optional<LinkedHashMultimap<String, String>> headers = params.getOptional("headers", new TypeReference<LinkedHashMultimap<String, String>>() {});

            if (content.isPresent()) {
                // TODO: support POST url encoded key/val and JSON encoding nested objects, files on disk etc
                ContentProvider contentProvider = new StringContentProvider(content.get());
                request.content(contentProvider, contentType.orNull());
            }

            if (headers.isPresent()) {
                for (Map.Entry<String, String> header : headers.get().entries()) {
                    request.header(header.getKey(), header.getValue());
                }
            }

            ContentResponse response = PollingRetryExecutor.pollingRetryExecutor(state, "request")
                    .withErrorMessage("HTTP request failed")
                    .run(s -> execute(request));

            return result(response);
        }

        private ContentResponse execute(Request req)
        {
            logger.debug("Sending HTTP request: {} {}", req.getMethod(), safeUri(req));
            ContentResponse res = send(req);
            logger.debug("Received HTTP response: {} {}: {}", req.getMethod(), safeUri(req), res);

            if (HttpStatus.isSuccess(res.getStatus())) {
                // 2xx: Success, we're done.
                return res;
            }
            else if (HttpStatus.isRedirection(res.getStatus())) {
                // 3xx: Redirect. We can get here if following redirects is disabled. We're done.
                return res;
            }
            else if (HttpStatus.isClientError(res.getStatus())) {
                // 4xx: The request is invalid for this resource. Fail hard without retrying.
                throw new TaskExecutionException("HTTP 4XX Client Error: " + requestStatus(req, res), ConfigElement.empty());
            }
            else if (res.getStatus() >= 500 && res.getStatus() < 600) {
                // 5xx: Server Error. This is hopefully ephemeral so retry.
                throw new RuntimeException("HTTP 5XX Server Error: " + requestStatus(req, res));
            }
            else {
                // Unknown status code. Treat as an ephemeral error.
                throw new RuntimeException("Unexpected HTTP status: " + requestStatus(req, res));
            }
        }

        private ContentResponse send(Request req)
        {
            ContentResponse res;
            try {
                res = req.send();
            }
            catch (InterruptedException e) {
                logger.debug("HTTP request interrupted: {}", req, e);
                throw Throwables.propagate(e);
            }
            catch (TimeoutException e) {
                logger.debug("HTTP request timeout: {}", req, e);
                throw Throwables.propagate(e);
            }
            catch (ExecutionException e) {
                logger.debug("HTTP request error: {}", req, e);
                if (e.getCause() != null) {
                    throw Throwables.propagate(e.getCause());
                }
                else {
                    throw Throwables.propagate(e);
                }
            }
            return res;
        }

        private String requestStatus(Request request, ContentResponse r)
        {
            URI safeUri = safeUri(request);
            return request.getMethod() + " " + safeUri + ": " + HttpStatus.getMessage(r.getStatus());
        }

        private URI safeUri(Request request)
        {
            URI uri = request.getURI();
            URI safeUri;
            try {
                safeUri = new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getRawPath(), uri.getRawQuery(), uri.getRawFragment());
            }
            catch (URISyntaxException e) {
                throw Throwables.propagate(e);
            }
            return safeUri;
        }

        private TaskResult result(ContentResponse response)
        {
            ConfigFactory cf = request.getConfig().getFactory();
            Config result = cf.create();
            Config bq = result.getNestedOrSetEmpty("http");
            bq.set("last_status", response.getStatus());
            return TaskResult.defaultBuilder(request)
                    .storeParams(result)
                    .addResetStoreParams(ConfigKey.of("http", "last_status"))
                    .build();
        }

        private HttpClient client()
        {
            SslContextFactory sslContextFactory = null;

            Optional<Boolean> insecure = params.getOptional("insecure", boolean.class);
            if (insecure.isPresent()) {
                sslContextFactory = new SslContextFactory(insecure.get());
            }

            HttpClient httpClient = new HttpClient(sslContextFactory);

            configureProxy(httpClient);

            boolean followRedirects = params.get("follow_redirects", boolean.class, true);

            httpClient.setFollowRedirects(followRedirects);
            httpClient.setMaxRedirects(maxRedirects);

            httpClient.setUserAgentField(new HttpField(
                    USER_AGENT, userAgent + ' ' + httpClient.getUserAgentField().getValue()));

            try {
                httpClient.start();
            }
            catch (Exception e) {
                throw new TaskExecutionException(e, TaskExecutionException.buildExceptionErrorConfig(e));
            }
            return httpClient;
        }

        private void configureProxy(HttpClient httpClient)
        {
            List<ProxyConfiguration.Proxy> proxies = httpClient.getProxyConfiguration().getProxies();
            Config userProxyConfig = params.getNestedOrGetEmpty("proxy");
            boolean userProxyEnabled = userProxyConfig.get("enabled", boolean.class, false);
            if (allowUserProxy && userProxyEnabled) {
                String host = userProxyConfig.get("host", String.class);
                int port = userProxyConfig.get("port", int.class);
                boolean tls = userProxyConfig.get("tls", boolean.class, false);
                proxies.add(new HttpProxy(new Origin.Address(host, port), tls));
            }
            else {
                if (systemProxy.isPresent()) {
                    proxies.add(systemProxy.get());
                }
                else {
                    configureEnvProxy("http", proxies);
                    configureEnvProxy("https", proxies);
                }
            }
        }

        private void stop(HttpClient httpClient)
        {
            try {
                httpClient.stop();
            }
            catch (Exception e) {
                logger.warn("Failed to stop http client", e);
            }
        }
    }

    private void configureEnvProxy(String scheme, List<ProxyConfiguration.Proxy> proxies)
    {
        Optional<ProxyConfig> proxyConfig = Proxies.proxyConfigFromEnv(scheme, env);
        if (proxyConfig.isPresent()) {
            ProxyConfig c = proxyConfig.get();
            proxies.add(new SchemeProxy(scheme, new Origin.Address(c.getHost(), c.getPort()), c.useSSL()));
        }
    }

    private class SchemeProxy
            extends HttpProxy
    {
        private final String scheme;

        public SchemeProxy(String scheme, Origin.Address address, boolean tls)
        {
            super(address, tls);
            this.scheme = scheme;
        }

        @Override
        public boolean matches(Origin origin)
        {
            return scheme.equals(origin.getScheme());
        }
    }
}
