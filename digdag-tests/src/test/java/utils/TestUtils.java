package utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import io.digdag.cli.Main;
import io.digdag.cli.YamlMapper;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Version;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import okhttp3.internal.tls.SslClient;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.io.FileUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;
import org.littleshoot.proxy.HttpFilters;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.HttpFiltersSourceAdapter;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.subethamail.smtp.AuthenticationHandler;
import org.subethamail.smtp.AuthenticationHandlerFactory;
import org.subethamail.smtp.RejectException;
import org.subethamail.wiser.Wiser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static com.google.common.primitives.Bytes.concat;
import static io.digdag.core.Version.buildVersion;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TestUtils
{
    private static final Logger logger = LoggerFactory.getLogger(TestUtils.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.registerModule(new GuavaModule());
        OBJECT_MAPPER.registerModule(new JacksonTimeModule());
        OBJECT_MAPPER.setInjectableValues(new InjectableValues.Std()
                .addValue(ObjectMapper.class, OBJECT_MAPPER));
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static final Pattern SESSION_ID_PATTERN = Pattern.compile("\\s*session id:\\s*(\\d+)\\s*");

    public static final Pattern ATTEMPT_ID_PATTERN = Pattern.compile("\\s*attempt id:\\s*(\\d+)\\s*");

    public static final Pattern PROJECT_ID_PATTERN = Pattern.compile("\\s*id:\\s*(\\d+)\\s*");

    public static CommandStatus main(String... args)
    {
        return main(buildVersion(), args);
    }

    public static CommandStatus main(InputStream in, String... args)
    {
        return main(buildVersion(), in, asList(args));
    }

    public static CommandStatus main(Map<String, String> env, String... args)
    {
        return main(env, ImmutableList.copyOf(args));
    }

    public static CommandStatus main(Map<String, String> env, Collection<String> args)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        InputStream in = new ByteArrayInputStream(new byte[0]);
        int code = main(env, buildVersion(), args, out, err, in);
        return CommandStatus.of(code, out.toByteArray(), err.toByteArray());
    }

    public static CommandStatus main(Collection<String> args)
    {
        return main(buildVersion(), args);
    }

    public static CommandStatus main(Version localVersion, String... args)
    {
        return main(localVersion, asList(args));
    }

    public static CommandStatus main(Version localVersion, Collection<String> args)
    {
        InputStream in = new ByteArrayInputStream(new byte[0]);
        return main(localVersion, in, args);
    }

    public static CommandStatus main(Version localVersion, InputStream in, Collection<String> args)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = main(ImmutableMap.of(), localVersion, args, out, err, in);
        return CommandStatus.of(code, out.toByteArray(), err.toByteArray());
    }

    public static int main(Map<String, String> env, Version localVersion, Collection<String> args, OutputStream out, OutputStream err, InputStream in)
    {
        try (
                PrintStream outp = new PrintStream(out, true, "UTF-8");
                PrintStream errp = new PrintStream(err, true, "UTF-8");
        ) {
            Main main = new Main(localVersion, env, outp, errp, in);
            return main.cli(args.stream().toArray(String[]::new));
        }
        catch (RuntimeException | UnsupportedEncodingException e) {
            e.printStackTrace();
            Assert.fail();
            throw Throwables.propagate(e);
        }
    }

    public static void copyResource(String resource, Path dest)
            throws IOException
    {
        if (Files.isDirectory(dest)) {
            Path name = Paths.get(resource).getFileName();
            copyResource(resource, dest.resolve(name));
        }
        else {
            try (InputStream input = Resources.getResource(resource).openStream()) {
                Files.copy(input, dest, REPLACE_EXISTING);
            }
        }
    }

    public static void fakeHome(String home, Action a)
            throws Exception
    {
        String orig = System.setProperty("user.home", home);
        try {
            Files.createDirectories(Paths.get(home).resolve(".config").resolve("digdag"));
            a.run();
        }
        finally {
            System.setProperty("user.home", orig);
        }
    }

    public static int findFreePort()
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static long getSessionId(CommandStatus startStatus)
    {
        Matcher matcher = SESSION_ID_PATTERN.matcher(startStatus.outUtf8());
        assertThat(matcher.find(), is(true));
        return Long.parseLong(matcher.group(1));
    }

    public static long getAttemptId(CommandStatus startStatus)
    {
        Matcher matcher = ATTEMPT_ID_PATTERN.matcher(startStatus.outUtf8());
        assertThat(matcher.find(), is(true));
        return Long.parseLong(matcher.group(1));
    }

    public static int getProjectId(CommandStatus pushStatus)
    {
        Matcher matcher = PROJECT_ID_PATTERN.matcher(pushStatus.outUtf8());
        assertThat(matcher.find(), is(true));
        return Integer.parseInt(matcher.group(1));
    }

    public static String getAttemptLogs(DigdagClient client, long attemptId)
            throws IOException
    {
        List<RestLogFileHandle> handles = client.getLogFileHandlesOfAttempt(attemptId);
        StringBuilder logs = new StringBuilder();
        for (RestLogFileHandle handle : handles) {
            try (InputStream s = new GZIPInputStream(client.getLogFile(attemptId, handle))) {
                logs.append(new String(ByteStreams.toByteArray(s), UTF_8));
            }
            catch (IOException ignore) {
                // XXX: the digdag client can return empty streams
            }
        }
        return logs.toString();
    }

    public static <T> org.hamcrest.Matcher<T> validUuid()
    {
        return new BaseMatcher<T>()
        {
            @Override
            public boolean matches(Object o)
            {
                if (!(o instanceof CharSequence)) {
                    return false;
                }
                String s = String.valueOf(o);
                try {
                    UUID uuid = UUID.fromString(s);
                    return true;
                }
                catch (Exception e) {
                    return false;
                }
            }

            @Override
            public void describeTo(Description description)
            {
                description.appendText("a valid uuid string");
            }
        };
    }

    public static void expect(TemporalAmount timeout, Callable<Boolean> condition)
            throws Exception
    {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().toEpochMilli() < deadline.toEpochMilli()) {
            if (condition.call()) {
                return;
            }
            Thread.sleep(1000);
        }

        fail("Timeout after: " + timeout);
    }

    public static <T> T expectValue(TemporalAmount timeout, Callable<T> condition)
            throws Exception
    {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().toEpochMilli() < deadline.toEpochMilli()) {
            try {
                T value = condition.call();
                if (value != null) {
                    return value;
                }
            }
            catch (Exception ignore) {
            }
            Thread.sleep(1000);
        }

        throw new AssertionError("Timeout after: " + timeout);
    }

    public static Callable<Boolean> attemptFailure(String endpoint, long attemptId)
    {
        return () -> {
            CommandStatus attemptsStatus = attempts(endpoint, attemptId);
            String output = attemptsStatus.outUtf8();
            if (output.contains("status: error")) {
                return true;
            }
            if (!output.contains("status: running")) {
                fail();
            }
            return false;
        };
    }

    public static Callable<Boolean> attemptSuccess(String endpoint, long attemptId)
    {
        return () -> {
            CommandStatus attemptsStatus = attempts(endpoint, attemptId);

            String output = attemptsStatus.outUtf8();
            if (output.contains("status: success")) {
                return true;
            }
            if (!output.contains("status: running")) {
                fail();
            }
            return false;
        };
    }

    public static CommandStatus attempts(String endpoint, long attemptId)
    {
        return main("attempts",
                "-c", "/dev/null",
                "-e", endpoint,
                String.valueOf(attemptId));
    }

    public static void createProject(Path project)
    {
        CommandStatus initStatus = main("init",
                "-c", "/dev/null",
                project.toString());
        assertThat(initStatus.code(), is(0));
    }

    public static long pushAndStart(String endpoint, Path project, String workflow)
            throws IOException
    {
        return pushAndStart(endpoint, project, workflow, ImmutableMap.of());
    }

    public static long pushAndStart(String endpoint, Path project, String workflow, Map<String, String> params)
            throws IOException
    {
        String projectName = project.getFileName().toString();

        // Push the project
        pushProject(endpoint, project, projectName);

        return startWorkflow(endpoint, projectName, workflow, params);
    }

    public static long startWorkflow(String endpoint, String projectName, String workflow, Map<String, String> params)
    {
        List<String> startCommand = new ArrayList<>(asList("start",
                "-c", "/dev/null",
                "-e", endpoint,
                projectName, workflow,
                "--session", "now"));

        params.forEach((k, v) -> startCommand.addAll(asList("-p", k + "=" + v)));

        // Start the workflow
        CommandStatus startStatus = main(startCommand);
        assertThat(startStatus.errUtf8(), startStatus.code(), is(0));

        return getAttemptId(startStatus);
    }

    public static int pushProject(String endpoint, Path project)
    {
        String projectName = project.getFileName().toString();
        return pushProject(endpoint, project, projectName);
    }

    public static int pushProject(String endpoint, Path project, String projectName)
    {
        CommandStatus pushStatus = main("push",
                "--project", project.toString(),
                projectName,
                "-c", "/dev/null",
                "-e", endpoint);
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        Matcher matcher = PROJECT_ID_PATTERN.matcher(pushStatus.outUtf8());
        boolean found = matcher.find();
        assertThat(found, is(true));
        return Integer.valueOf(matcher.group(1));
    }

    public static void addWorkflow(Path project, String resource)
            throws IOException
    {
        addResource(project, resource);
    }

    public static void addResource(Path project, String resource)
            throws IOException
    {
        Path path = Paths.get(resource);
        addResource(project, resource, path.getFileName().toString());
    }

    public static void addWorkflow(Path project, String resource, String workflowName)
            throws IOException
    {
        addResource(project, resource, workflowName);
    }

    public static void addResource(Path project, String resource, String workflowName)
            throws IOException
    {
        copyResource(resource, project.resolve(workflowName));
    }

    public static void runWorkflow(TemporaryFolder folder, String resource, Map<String, String> params)
            throws IOException
    {
        runWorkflow(folder, resource, params, ImmutableMap.of());
    }

    public static void runWorkflow(TemporaryFolder folder, String resource, Map<String, String> params, Map<String, String> config)
            throws IOException
    {
        runWorkflow(folder, resource, params, config, 0);
    }

    public static void runWorkflow(TemporaryFolder folder, String resource, Map<String, String> params, Map<String, String> config, int expectedStatus)
            throws IOException
    {
        Path workflow = Paths.get(resource);
        Path tempdir = folder.newFolder().toPath();
        Path file = tempdir.resolve(workflow.getFileName());
        Path configFile = folder.newFolder().toPath().resolve("config");
        List<String> configLines = config.entrySet().stream()
                .map(e -> e.getKey() + " = " + e.getValue())
                .collect(Collectors.toList());
        Files.write(configFile, configLines);
        List<String> runCommand = new ArrayList<>(asList("run",
                "-c", configFile.toAbsolutePath().normalize().toString(),
                "-o", tempdir.toString(),
                "--project", tempdir.toString(),
                workflow.getFileName().toString()));
        params.forEach((k, v) -> runCommand.addAll(asList("-p", k + "=" + v)));
        try {
            copyResource(resource, file);
            CommandStatus status = main(runCommand);
            assertThat(status.errUtf8(), status.code(), is(expectedStatus));
        }
        finally {
            FileUtils.deleteQuietly(tempdir.toFile());
        }
    }

    public static ObjectMapper objectMapper()
    {
        return OBJECT_MAPPER;
    }

    public static YamlMapper yamlMapper()
    {
        return new YamlMapper(OBJECT_MAPPER);
    }

    public static ConfigFactory configFactory()
    {
        return new ConfigFactory(objectMapper());
    }

    public static Wiser startMailServer(String hostname, String user, String password)
    {
        AuthenticationHandlerFactory authenticationHandlerFactory = new AuthenticationHandlerFactory()
        {
            @Override
            public List<String> getAuthenticationMechanisms()
            {
                return ImmutableList.of("PLAIN");
            }

            @Override
            public AuthenticationHandler create()
            {
                return new AuthenticationHandler()
                {

                    private String identity;

                    @Override
                    public String auth(String clientInput)
                            throws RejectException
                    {
                        String prefix = "AUTH PLAIN ";
                        if (!clientInput.startsWith(prefix)) {
                            throw new RejectException();
                        }
                        String credentialsBase64 = clientInput.substring(prefix.length());
                        byte[] credentials = Base64.getDecoder().decode(credentialsBase64);

                        // [authzid] UTF8NUL authcid UTF8NUL passwd
                        byte[] expectedCredentials = concat(
                                user.getBytes(UTF_8),
                                new byte[] {0},
                                user.getBytes(UTF_8),
                                new byte[] {0},
                                password.getBytes(UTF_8)
                        );

                        if (!Arrays.equals(credentials, expectedCredentials)) {
                            throw new RejectException();
                        }

                        this.identity = user;
                        return null;
                    }

                    @Override
                    public Object getIdentity()
                    {
                        return identity;
                    }
                };
            }
        };
        return startMailServer(hostname, authenticationHandlerFactory);
    }

    public static Wiser startMailServer(String hostname, AuthenticationHandlerFactory authenticationHandlerFactory)
    {
        Wiser server = new Wiser();
        server.getServer().setAuthenticationHandlerFactory(authenticationHandlerFactory);
        server.setHostname(hostname);
        server.setPort(0);
        server.start();
        return server;
    }

    public static MockWebServer startMockWebServer()
    {
        return startMockWebServer(false);
    }

    public static MockWebServer startMockWebServer(boolean https)
    {
        MockWebServer server = new MockWebServer();
        server.setDispatcher(new NopDispatcher());
        if (https) {
            server.useHttps(SslClient.localhost().socketFactory, false);
        }
        try {
            server.start(0);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return server;
    }

    public static HttpProxyServer startRequestTrackingProxy(final List<FullHttpRequest> requests)
    {
        return DefaultHttpProxyServer
                .bootstrap()
                .withPort(0)
                .withFiltersSource(new HttpFiltersSourceAdapter()
                {
                    @Override
                    public int getMaximumRequestBufferSizeInBytes()
                    {
                        return 1024 * 1024;
                    }

                    @Override
                    public HttpFilters filterRequest(HttpRequest httpRequest, ChannelHandlerContext channelHandlerContext)
                    {
                        return new HttpFiltersAdapter(httpRequest)
                        {
                            @Override
                            public HttpResponse clientToProxyRequest(HttpObject httpObject)
                            {
                                assert httpObject instanceof FullHttpRequest;
                                requests.add(((FullHttpRequest) httpObject).copy());
                                return null;
                            }
                        };
                    }
                }).start();
    }

    /**
     * Starts a proxy that fails all requests except every {@code failures}'th request per unique (method, uri) pair.
     */
    public static HttpProxyServer startRequestFailingProxy(int failures)
    {
        ConcurrentMap<String, List<FullHttpRequest>> requests = new ConcurrentHashMap<>();
        return startRequestFailingProxy(failures, requests);
    }

    /**
     * Starts a proxy that fails all requests except every {@code failures}'th request per unique (method, uri) pair.
     */
    public static HttpProxyServer startRequestFailingProxy(final int failures, final ConcurrentMap<String, List<FullHttpRequest>> requests)
    {
        return startRequestFailingProxy(failures, requests, INTERNAL_SERVER_ERROR);
    }

    /**
     * Starts a proxy that fails all requests except every {@code failures}'th request per unique (method, uri) pair.
     */
    public static HttpProxyServer startRequestFailingProxy(final int failures, final ConcurrentMap<String, List<FullHttpRequest>> requests, final HttpResponseStatus error)
    {
        return DefaultHttpProxyServer
                .bootstrap()
                .withPort(0)
                .withFiltersSource(new HttpFiltersSourceAdapter()
                {
                    @Override
                    public int getMaximumRequestBufferSizeInBytes()
                    {
                        return 1024 * 1024;
                    }

                    @Override
                    public HttpFilters filterRequest(HttpRequest httpRequest, ChannelHandlerContext channelHandlerContext)
                    {
                        return new HttpFiltersAdapter(httpRequest)
                        {
                            @Override
                            public HttpResponse clientToProxyRequest(HttpObject httpObject)
                            {
                                assert httpObject instanceof FullHttpRequest;
                                FullHttpRequest fullHttpRequest = (FullHttpRequest) httpObject;
                                String key = fullHttpRequest.getMethod() + " " + fullHttpRequest.getUri();
                                List<FullHttpRequest> keyedRequests = requests.computeIfAbsent(key, k -> new ArrayList<>());
                                int n;
                                synchronized (keyedRequests) {
                                    keyedRequests.add(fullHttpRequest.copy());
                                    n = keyedRequests.size();
                                }
                                if (n % failures != 0) {
                                    logger.info("Simulating {} for request: {}", error, key);
                                    HttpResponse response = new DefaultFullHttpResponse(httpRequest.getProtocolVersion(), error);
                                    response.headers().set(CONNECTION, CLOSE);
                                    return response;
                                }
                                else {
                                    logger.info("Passing request: {}", key);
                                    return null;
                                }
                            }
                        };
                    }
                }).start();
    }
}
