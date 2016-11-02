package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
import io.digdag.core.Environment;
import io.digdag.core.Version;
import io.digdag.spi.ImmutableTaskResult;
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
    private final int maxStoredResponseContentSize;

    @Inject
    public HttpOperatorFactory(Config systemConfig, @Environment Map<String, String> env)
    {
        this.allowUserProxy = systemConfig.get("config.http.allow_user_proxy", boolean.class, true);
        this.systemProxy = systemProxy(systemConfig);
        this.maxRedirects = systemConfig.get("config.http.max_redirects", int.class, 8);
        this.maxStoredResponseContentSize = systemConfig.get("config.http.max_stored_response_content_size", int.class, 64 * 1024);
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
        private final String method;
        private final boolean retry;

        private HttpOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request);
            this.state = TaskState.of(request);
            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("http"));
            this.method = params.get("method", String.class, "GET").toUpperCase();
            this.retry = params.getOptional("retry", boolean.class)
                    .or(defaultRetry(method));
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
            boolean storeContent = params.get("store_content", boolean.class, false);

            if (content.isPresent()) {
                // TODO: support POST url encoded key/val and JSON encoding nested objects, files on disk etc
                ContentProvider contentProvider = new StringContentProvider(content.get());
                request.content(contentProvider, contentType.orNull());
            }

            LinkedHashMultimap<String, String> headers = headers();
            for (Map.Entry<String, String> header : headers.entries()) {
                request.header(header.getKey(), header.getValue());
            }

            ContentResponse response = PollingRetryExecutor.pollingRetryExecutor(state, "request")
                    .withErrorMessage("HTTP request failed")
                    .run(s -> execute(request));

            return result(response, storeContent);
        }

        private LinkedHashMultimap<String, String> headers()
        {
            List<JsonNode> entries = params.getListOrEmpty("headers", JsonNode.class);
            LinkedHashMultimap headers = LinkedHashMultimap.create();
            for (JsonNode entry : entries) {
                if (!entry.isObject()) {
                    throw new ConfigException("Invalid header: " + entry);
                }
                ObjectNode o = (ObjectNode) entry;
                if (o.size() != 1) {
                    throw new ConfigException("Invalid header: " + entry);
                }
                String name = o.fieldNames().next();
                String value = o.get(name).asText();
                headers.put(name, value);
            }

            return headers;
        }

        private ContentResponse execute(Request req)
        {
            URI safeUri = safeUri(req);

            logger.debug("Sending HTTP request: {} {}", req.getMethod(), safeUri);
            ContentResponse res = send(req);
            logger.debug("Received HTTP response: {} {}: {}", req.getMethod(), safeUri, res);

            if (HttpStatus.isSuccess(res.getStatus())) {
                // 2xx: Success, we're done.
                return res;
            }
            else if (HttpStatus.isRedirection(res.getStatus())) {
                // 3xx: Redirect. We can get here if following redirects is disabled. We're done.
                return res;
            }
            else if (HttpStatus.isClientError(res.getStatus())) {
                switch (res.getStatus()) {
                    case HttpStatus.REQUEST_TIMEOUT_408:
                    case HttpStatus.TOO_MANY_REQUESTS_429:
                        // Retry these.
                        throw new RuntimeException("Failed HTTP request: " + requestStatus(req, res));
                    default:
                        // 4xx: The request is invalid for this resource. Fail hard without retrying.
                        throw new TaskExecutionException("HTTP 4XX Client Error: " + requestStatus(req, res), ConfigElement.empty());
                }
            }
            else if (res.getStatus() >= 500 && res.getStatus() < 600) {
                // 5xx: Server Error. This is hopefully ephemeral.
                throw ephemeralError("HTTP 5XX Server Error: " + requestStatus(req, res));
            }
            else {
                // Unknown status code. Treat as an ephemeral error.
                throw ephemeralError("Unexpected HTTP status: " + requestStatus(req, res));
            }
        }

        private RuntimeException ephemeralError(String message)
        {
            // Safe to retry ephemeral errors for this request?
            if (retry) {
                // Yes. Retry this request.
                return new RuntimeException(message);
            }
            else {
                // No, so fail hard.
                return new TaskExecutionException(message, ConfigElement.empty());
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

        private TaskResult result(ContentResponse response, boolean storeContent)
        {
            ConfigFactory cf = request.getConfig().getFactory();
            Config result = cf.create();
            Config http = result.getNestedOrSetEmpty("http");
            http.set("last_status", response.getStatus());

            ImmutableTaskResult.Builder builder = TaskResult.defaultBuilder(request)
                    .addResetStoreParams(ConfigKey.of("http", "last_status"));

            if (storeContent) {
                String content = response.getContentAsString();
                if (content.length() > maxStoredResponseContentSize) {
                    throw new TaskExecutionException("Response content too large: " + content.length() + " > " + maxStoredResponseContentSize, ConfigElement.empty());
                }
                http.set("last_content", content);
                builder.addResetStoreParams(ConfigKey.of("http", "last_content"));
            }

            return builder
                    .storeParams(result)
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

    private static boolean defaultRetry(String method)
    {
        switch (method) {
            case "GET":
            case "HEAD":
            case "OPTIONS":
            case "TRACE":
                return true;
            default:
                return false;
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
