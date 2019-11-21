package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Throwables;
import com.google.common.base.Optional;
import com.google.common.collect.LinkedHashMultimap;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
import io.digdag.core.Environment;
import io.digdag.client.DigdagVersion;
import io.digdag.spi.ImmutableTaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.standards.Proxies;
import io.digdag.standards.operator.state.PollingRetryExecutor;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;
import io.digdag.util.UserSecretTemplate;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.HttpResponseException;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.BasicAuthentication;
import org.eclipse.jetty.client.util.FormContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.Fields;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
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
        this.userAgent = systemConfig.get("config.http.user_agent", String.class, "Digdag/" + DigdagVersion.buildVersion());
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
    public Operator newOperator(OperatorContext context)
    {
        return new HttpOperator(context);
    }

    class HttpOperator
            extends BaseOperator
    {
        private final TaskState state;
        final Config params;
        final String method;
        final boolean retry;
        final long timeout;
        final SecretProvider httpSecrets;

        protected HttpOperator(OperatorContext context)
        {
            super(context);
            this.state = TaskState.of(request);
            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("http"));
            this.method = params.get("method", String.class, "GET").toUpperCase();
            this.retry = params.getOptional("retry", boolean.class)
                    .or(defaultRetry(method));
            this.timeout = params.get("timeout", Long.class, 30L);
            this.httpSecrets = context.getSecrets().getSecrets("http");
        }

        @Override
        public TaskResult runTask()
        {
            HttpClient client = client();
            try {
                return run(client);
            }
            finally {
                stop(client);
            }
        }

        private TaskResult run(HttpClient httpClient)
        {
            Optional<String> secretUri = httpSecrets.getSecretOptional("uri");
            String rawUri;
            boolean uriIsSecret;
            if (secretUri.isPresent()) {
                uriIsSecret = true;
                rawUri = secretUri.get();
            }
            else {
                UserSecretTemplate uriTemplate = UserSecretTemplate.of(params.get("_command", String.class));
                uriIsSecret = uriTemplate.containsSecrets();
                rawUri = uriTemplate.format(context.getSecrets());
            }
            URI uri = URI.create(rawUri);

            boolean storeContent = params.get("store_content", boolean.class, false);

            ContentResponse response = runHttp(httpClient, uri, uriIsSecret);
            return result(response, storeContent);
        }

        ContentResponse runHttp(HttpClient httpClient, URI uri, boolean uriIsSecret)
        {
            Optional<String> user = httpSecrets.getSecretOptional("user");
            Optional<String> authorization = httpSecrets.getSecretOptional("authorization");

            Request request = httpClient.newRequest(uri)
                    .method(method)
                    .timeout(timeout, SECONDS);

            if (authorization.isPresent()) {
                request.header(AUTHORIZATION, authorization.get());
            }
            else if (user.isPresent()) {
                Optional<String> password = httpSecrets.getSecretOptional("password");
                httpClient.getAuthenticationStore().addAuthenticationResult(new BasicAuthentication.BasicResult(uri, user.get(), password.or("")));
            }

            Optional<JsonNode> content = params.getOptional("content", JsonNode.class);
            Optional<String> contentFormat = params.getOptional("content_format", String.class).transform(s -> s.toLowerCase(Locale.ROOT));
            Optional<String> contentType = params.getOptional("content_type", String.class);

            if (content.isPresent()) {
                // TODO: support files on disk etc
                request.content(contentProvider(content.get(), contentFormat, contentType, context.getSecrets()));
            }

            LinkedHashMultimap<String, String> headers = headers();
            for (Map.Entry<String, String> header : headers.entries()) {
                request.header(header.getKey(), header.getValue());
            }

            configureQueryParameters(request);

            ContentResponse response = PollingRetryExecutor.pollingRetryExecutor(state, "request")
                    .withErrorMessage("HTTP request failed")
                    .run(s -> execute(request, uriIsSecret));

            return response;
        }

        private void configureQueryParameters(Request request)
        {
            try {
                Config queryParameters = params.parseNestedOrGetEmpty("query");
                for (String name : queryParameters.getKeys()) {
                    request.param(
                            UserSecretTemplate.of(name).format(context.getSecrets()),
                            UserSecretTemplate.of(queryParameters.get(name, String.class)).format(context.getSecrets()));
                }
            }
            catch (ConfigException e) {
                Optional<String> query = params.getOptional("query", String.class)
                        .transform(s -> UserSecretTemplate.of(s).format(context.getSecrets()));
                if (query.isPresent()) {
                    List<NameValuePair> parameters = URLEncodedUtils.parse(query.get(), UTF_8);
                    for (NameValuePair parameter : parameters) {
                        request.param(parameter.getName(), parameter.getValue());
                    }
                }
            }
        }

        private LinkedHashMultimap<String, String> headers()
        {
            List<JsonNode> entries = params.getListOrEmpty("headers", JsonNode.class);
            LinkedHashMultimap<String, String> headers = LinkedHashMultimap.create();
            for (JsonNode entry : entries) {
                if (!entry.isObject()) {
                    throw new ConfigException("Invalid header: " + entry);
                }
                ObjectNode o = (ObjectNode) entry;
                if (o.size() != 1) {
                    throw new ConfigException("Invalid header: " + entry);
                }
                String key = o.fieldNames().next();
                String name = UserSecretTemplate.of(key).format(context.getSecrets());
                String value = UserSecretTemplate.of(o.get(key).asText()).format(context.getSecrets());
                headers.put(name, value);
            }

            return headers;
        }

        private ContentResponse execute(Request req, boolean uriIsSecret)
        {
            String safeUri = safeUri(req, uriIsSecret);

            logger.info("Sending HTTP request: {} {}", req.getMethod(), safeUri);
            ContentResponse res;
            try {
                res = send(req);
            }
            catch (HttpResponseException e) {
                throw error(req, uriIsSecret, e.getResponse());
            }
            catch (RuntimeException e) {
                logger.warn("Exception without response: {} {}", req.getMethod(), safeUri);
                if (retry) {
                    throw e;
                }
                else {
                    throw new TaskExecutionException(e);
                }
            }

            logger.info("Received HTTP response: {} {}: {}", req.getMethod(), safeUri, res);

            if (HttpStatus.isSuccess(res.getStatus())) {
                // 2xx: Success, we're done.
                return res;
            }
            else if (HttpStatus.isRedirection(res.getStatus())) {
                // 3xx: Redirect. We can get here if following redirects is disabled. We're done.
                return res;
            }
            else {
                throw error(req, uriIsSecret, res);
            }
        }

        private RuntimeException error(Request req, boolean uriIsSecret, Response res)
        {
            if (HttpStatus.isClientError(res.getStatus())) {
                switch (res.getStatus()) {
                    case HttpStatus.REQUEST_TIMEOUT_408:
                    case HttpStatus.TOO_MANY_REQUESTS_429:
                        // Retry these.
                        return new RuntimeException("Failed HTTP request: " + requestStatus(req, res, uriIsSecret));
                    default:
                        // 4xx: The request is invalid for this resource. Fail hard without retrying.
                        return new TaskExecutionException("HTTP 4XX Client Error: " + requestStatus(req, res, uriIsSecret));
                }
            }
            else if (res.getStatus() >= 500 && res.getStatus() < 600) {
                // 5xx: Server Error. This is hopefully ephemeral.
                return ephemeralError("HTTP 5XX Server Error: " + requestStatus(req, res, uriIsSecret));
            }
            else {
                // Unknown status code. Treat as an ephemeral error.
                return ephemeralError("Unexpected HTTP status: " + requestStatus(req, res, uriIsSecret));
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
                return new TaskExecutionException(message);
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

        private String requestStatus(Request request, Response r, boolean uriIsSecret)
        {
            String safeUri = safeUri(request, uriIsSecret);
            return request.getMethod() + " " + safeUri + ": " + r.getStatus() + " " + HttpStatus.getMessage(r.getStatus());
        }

        private String safeUri(Request request, boolean uriIsSecret)
        {
            URI uri = request.getURI();
            if (uriIsSecret) {
                return uri.getScheme() + "://***";
            }
            URI safeUri;
            try {
                safeUri = new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getRawPath(), uri.getRawQuery(), uri.getRawFragment());
            }
            catch (URISyntaxException e) {
                throw Throwables.propagate(e);
            }
            return safeUri.toString();
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
                    throw new TaskExecutionException("Response content too large: " + content.length() + " > " + maxStoredResponseContentSize);
                }
                http.set("last_content", content);
                builder.addResetStoreParams(ConfigKey.of("http", "last_content"));
            }

            return builder
                    .storeParams(result)
                    .build();
        }

        HttpClient client()
        {
            boolean insecure = params.get("insecure", boolean.class, false);

            HttpClient httpClient = new HttpClient(new SslContextFactory(insecure));

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
                throw new TaskExecutionException(e);
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

        void stop(HttpClient httpClient)
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

    private static ContentProvider contentProvider(JsonNode content, Optional<String> contentFormat, Optional<String> contentType, SecretProvider secrets)
    {
        // content-type can be inferred from the actual content and content_format parameters.

        // TODO: also infer format is from content-type?

        // content format  format' content-type'                           comment
        // --------------------------------------------------------------------------------------------------------------
        // scalar  -       text    text/plain                              Raw value as text. Strings are not hyphenated.
        // scalar  text    text    text/plain                              Raw value as text. Strings are not hyphenated.
        // scalar  json    json    application/json
        // scalar  form    form    application/x-www-form-urlencoded       Raw value url encoded
        // ary/obj -       json    application/json
        // ary/obj json    json    application/json
        // ary/obj text                                                    ERROR
        // array   form                                                    ERROR
        // object  form    form    application/x-www-form-urlencoded

        String nodeType = content.getNodeType().name().toLowerCase(Locale.ROOT);

        if (content.isContainerNode()) {
            // Object or Array
            String format = contentFormat.or("json");
            switch (format) {
                case "json":
                    return new StringContentProvider(contentType.or("application/json"), resolveSecrets(content, secrets).toString(), UTF_8);
                case "form":
                    if (content.isArray()) {
                        throw invalidContentFormat(format, nodeType);
                    }
                    else {
                        return new FormContentProvider(formFields((ObjectNode) resolveSecrets(content, secrets)));
                    }
                default:
                    throw invalidContentFormat(format, nodeType);
            }
        }
        else {
            // Scalar
            String format = contentFormat.or("text");
            switch (format) {
                case "text":
                    return new StringContentProvider(contentType.or("plain/text"), resolveSecrets(content, secrets).asText(), UTF_8);
                case "json":
                    return new StringContentProvider(contentType.or("application/json"), resolveSecrets(content, secrets).toString(), UTF_8);
                default:
                    throw invalidContentFormat(format, nodeType);
            }
        }
    }

    private static JsonNode resolveSecrets(JsonNode node, SecretProvider secrets)
    {
        if (node.isObject()) {
            ObjectNode object = (ObjectNode) node;
            ObjectNode newObject = object.objectNode();
            object.fields().forEachRemaining(entry -> newObject.set(
                    UserSecretTemplate.of(entry.getKey()).format(secrets),
                    resolveSecrets(entry.getValue(), secrets)));
            return newObject;
        }
        else if (node.isArray()) {
            ArrayNode array = (ArrayNode) node;
            ArrayNode newArray = array.arrayNode();
            array.elements().forEachRemaining(element -> newArray.add(resolveSecrets(element, secrets)));
            return newArray;
        }
        else if (node.isTextual()) {
            return new TextNode(UserSecretTemplate.of(node.textValue()).format(secrets));
        }
        else {
            return node;
        }
    }

    private static ConfigException invalidContentFormat(String format, String nodeType)
    {
        return new ConfigException("Invalid content format for " + nodeType + "s: '" + format + "'");
    }

    private static Fields formFields(ObjectNode content)
    {
        Fields fields = new Fields();
        content.fields().forEachRemaining(f -> {
            if (f.getValue().isContainerNode()) {
                throw new ConfigException("Invalid form content field value: " + f.getValue().toString());
            }
            fields.add(f.getKey(), f.getValue().asText());
        });
        return fields;
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
