package utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import com.google.inject.Injector;
import com.treasuredata.client.TDApiRequest;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientConfig;
import com.treasuredata.client.TDHttpClient;
import io.digdag.cli.Main;
import io.digdag.cli.Run;
import io.digdag.cli.YamlMapper;
import io.digdag.client.DigdagClient;
import io.digdag.client.Version;
import io.digdag.client.api.Id;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.spi.SecretProvider;
import io.digdag.standards.operator.td.BaseTDClientFactory;
import io.digdag.standards.operator.td.TDClientFactory;
import io.digdag.standards.operator.td.TDOperator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.tls.HandshakeCertificates;
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

import javax.net.ssl.SSLSocketFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static com.google.common.primitives.Bytes.concat;
import static io.digdag.util.RetryExecutor.retryExecutor;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Arrays.asList;
import static okhttp3.tls.internal.TlsUtil.localhost;
import static org.hamcrest.CoreMatchers.containsString;
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
        return main(LocalVersion.of(), args);
    }

    public static CommandStatus main(InputStream in, String... args)
    {
        return main(LocalVersion.of(), in, asList(args));
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
        int code = main(env, LocalVersion.of(), args, out, err, in);
        return CommandStatus.of(code, out.toByteArray(), err.toByteArray());
    }

    public static CommandStatus main(Collection<String> args)
    {
        return main(LocalVersion.of(), args);
    }

    public static CommandStatus main(LocalVersion localVersion, String... args)
    {
        return main(localVersion, asList(args));
    }

    public static CommandStatus main(LocalVersion localVersion, Collection<String> args)
    {
        InputStream in = new ByteArrayInputStream(new byte[0]);
        return main(localVersion, in, args);
    }

    public static CommandStatus main(LocalVersion localVersion, InputStream in, Collection<String> args)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = main(ImmutableMap.of(), localVersion, args, out, err, in);
        return CommandStatus.of(code, out.toByteArray(), err.toByteArray());
    }

    public static int main(Map<String, String> env, LocalVersion localVersion, Collection<String> args, OutputStream out, OutputStream err, InputStream in)
    {
        return main(env, localVersion, args, out, err, in, null);
    }

    public static int main(
            Map<String, String> env,
            LocalVersion localVersion,
            Collection<String> args,
            OutputStream out,
            OutputStream err,
            InputStream in,
            RecordableWorkflow.CustomMain.CommandAdder commandAdder)
    {
        try (
                PrintStream outp = new PrintStream(out, true, "UTF-8");
                PrintStream errp = new PrintStream(err, true, "UTF-8");
        ) {
            Main main;
            if (commandAdder != null) {
                main = new RecordableWorkflow.CustomMain(localVersion.getVersion(), env, outp, errp, in, commandAdder);
            }
            else {
                main = new Main(localVersion.getVersion(), env, outp, errp, in);
            }
            System.setProperty("io.digdag.cli.versionCheckMode", localVersion.isBatchModeCheck() ? "batch" : "interactive");
            return main.cli(args.stream().toArray(String[]::new));
        }
        catch (RuntimeException | UnsupportedEncodingException e) {
            e.printStackTrace();
            Assert.fail();
            throw Throwables.propagate(e);
        }
    }

    public static class RecordableWorkflow
    {
        public static class ApiCallRecord
        {
            public final TDApiRequest request;
            public final Optional<String> apikeyCache;

            public ApiCallRecord(TDApiRequest request, Optional<String> apikeyCache)
            {
                this.request = request;
                this.apikeyCache = apikeyCache;
            }
        }

        public static class TDHttpClientRecorder
        {
            private final List<ApiCallRecord> apiCallRecords = new ArrayList<>();

            public void record(ApiCallRecord apiCallRecord)
            {
                apiCallRecords.add(apiCallRecord);
            }

            public List<ApiCallRecord> apiCallRecords()
            {
                return apiCallRecords;
            }
        }

        static class RecordableTDHttpClient
                extends TDHttpClient
        {
            private final TDHttpClientRecorder recorder;

            public RecordableTDHttpClient(TDClientConfig config, TDHttpClientRecorder recorder)
            {
                super(config);
                this.recorder = recorder;
            }

            @Override
            public <Result> Result call(TDApiRequest apiRequest, Optional<String> apiKeyCache, final JavaType resultType)
            {
                recorder.record(new ApiCallRecord(apiRequest, apiKeyCache.or(config.apiKey)));
                return super.call(apiRequest, apiKeyCache, resultType);
            }
        }

        static class RecordableTDClient
                extends TDClient
        {
            public RecordableTDClient(TDClientConfig config, TDHttpClientRecorder recorder)
            {
                super(config, new RecordableTDHttpClient(config, recorder), Optional.absent());
            }
        }

        static class RecordableTDClientFactory
                extends TDClientFactory
        {
            private final TDHttpClientRecorder recorder;

            public RecordableTDClientFactory(TDHttpClientRecorder recorder)
            {
                this.recorder = recorder;
            }

            @Override
            public TDClient createClient(
                    TDOperator.SystemDefaultConfig systemDefaultConfig,
                    Map<String, String> env,
                    Config params,
                    SecretProvider secrets)
            {
                TDClientConfig clientConfig = clientBuilderFromConfig(systemDefaultConfig, env, params, secrets).buildConfig();
                return new RecordableTDClient(clientConfig, recorder);
            }
        }

        static class RecordableRun
                extends Run
        {
            private TDHttpClientRecorder recorder;

            public void setRecorder(TDHttpClientRecorder recorder)
            {
                this.recorder = recorder;
            }

            @Override
            protected DigdagEmbed.Bootstrap setupBootstrap(Properties systemProps)
            {
                return super.setupBootstrap(systemProps)
                        .overrideModulesWith((binder) ->
                                binder.bind(BaseTDClientFactory.class).toInstance(new RecordableTDClientFactory(recorder)));
            }
        }

        static class CustomMain
                extends Main
        {
            interface CommandAdder
            {
                void addCommand(JCommander jc, Injector injector);
            }

            private final CommandAdder commandAdder;

            public CustomMain(
                    Version version,
                    Map<String, String> env,
                    PrintStream out,
                    PrintStream err,
                    InputStream in,
                    CommandAdder commandAdder)
            {
                super(version, env, out, err, in);
                this.commandAdder = commandAdder;
            }

            @Override
            protected void addCommands(JCommander jc, Injector injector)
            {
                super.addCommands(jc, injector);
                commandAdder.addCommand(jc, injector);
            }
        }

        public static class CommandStatusAndRecordedApiCalls
        {
            public final CommandStatus commandStatus;
            public final List<ApiCallRecord> apiCallRecords;

            public CommandStatusAndRecordedApiCalls(CommandStatus commandStatus, List<ApiCallRecord> apiCallRecords)
            {
                this.commandStatus = commandStatus;
                this.apiCallRecords = apiCallRecords;
            }
        }

        public static CommandStatusAndRecordedApiCalls mainWithRecordableRun(Map<String, String> env, Collection<String> args)
        {
            InputStream in = new ByteArrayInputStream(new byte[0]);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayOutputStream err = new ByteArrayOutputStream();

            TDHttpClientRecorder recorder = new TDHttpClientRecorder();
            CustomMain.CommandAdder commandAdder = (jc, injector) -> {
                RecordableRun instance = injector.getInstance(RecordableRun.class);
                instance.setRecorder(recorder);
                jc.addCommand("recordable_run", instance);
            };

            int code = TestUtils.main(env, LocalVersion.of(), args, out, err, in, commandAdder);

            return new CommandStatusAndRecordedApiCalls(
                    CommandStatus.of(code, out.toByteArray(), err.toByteArray()),
                    recorder.apiCallRecords());
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
        withSystemProperty("user.home", home, a);
    }

    public static void withSystemProperty(String key, String value, Action a)
            throws Exception
    {
        withSystemProperties(ImmutableMap.of(key, value), a);
    }

    private static final Object NOT_PRESENT = new Object();

    public static void withSystemProperties(Map<String, String> props, Action a)
            throws Exception
    {
        Properties systemProperties = System.getProperties();
        Map<String, Object> prev = new HashMap<>();
        for (String key : props.keySet()) {
            if (systemProperties.contains(key)) {
                prev.put(key, systemProperties.getProperty(key));
            } else {
                prev.put(key, NOT_PRESENT);
            }
        }
        try {
            systemProperties.putAll(props);
            a.run();
        }
        finally {
            for (Map.Entry<String, Object> e : prev.entrySet()) {
                if (e.getValue() == NOT_PRESENT) {
                    systemProperties.remove(e.getKey());
                } else {
                    systemProperties.put(e.getKey(), e.getValue());
                }
            }
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

    public static Id getSessionId(CommandStatus startStatus)
    {
        Matcher matcher = SESSION_ID_PATTERN.matcher(startStatus.outUtf8());
        assertThat(matcher.find(), is(true));
        return Id.of(matcher.group(1));
    }

    public static Id getAttemptId(CommandStatus startStatus)
    {
        Matcher matcher = ATTEMPT_ID_PATTERN.matcher(startStatus.outUtf8());
        assertThat(matcher.find(), is(true));
        return Id.of(matcher.group(1));
    }

    public static Id getProjectId(CommandStatus pushStatus)
    {
        Matcher matcher = PROJECT_ID_PATTERN.matcher(pushStatus.outUtf8());
        assertThat(matcher.find(), is(true));
        return Id.of(matcher.group(1));
    }

    public static String getAttemptLogs(DigdagClient client, Id attemptId)
            throws IOException, InterruptedException
    {
        List<RestLogFileHandle> handles = client.getLogFileHandlesOfAttempt(attemptId).getFiles();
        StringBuilder logs = new StringBuilder();
        for (RestLogFileHandle handle : handles) {
            if (handle.getDirect().isPresent()) {
                System.out.println("===");
//                System.out.println(handle.getFileName());
//                System.out.println("getDirect.isPresent():" + handle.getDirect().isPresent());
                System.out.println("getFileSize():" + handle.getFileSize());
                System.out.println("===");
//                for (int i = 0; i < 300; i++) {
//                    if (handle.getFileSize() > 0) {
//                        break;
//                    }
//                    Thread.sleep(1000);
//                }
            }
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
        expect(timeout, condition, Duration.ofSeconds(5));
    }

    public static void expect(TemporalAmount timeout, Callable<Boolean> condition, Duration interval)
            throws Exception
    {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().toEpochMilli() < deadline.toEpochMilli()) {
            if (condition.call()) {
                return;
            }
            Thread.sleep(interval.toMillis());
        }

        fail("Timeout after: " + timeout);
    }

    public static <T> T expectValue(TemporalAmount timeout, Callable<T> condition)
            throws Exception
    {
        return expectValue(timeout, condition, Duration.ofSeconds(5));
    }

    public static <T> T expectValue(TemporalAmount timeout, Callable<T> condition, Duration interval)
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
            Thread.sleep(interval.toMillis());
        }

        throw new AssertionError("Timeout after: " + timeout);
    }

    public static Callable<Boolean> attemptFailure(String endpoint, Id attemptId)
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

    public static Callable<Boolean> attemptSuccess(String endpoint, Id attemptId)
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

    public static CommandStatus attempts(String endpoint, Id attemptId)
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

    public static Id pushAndStart(String endpoint, Path project, String workflow)
            throws IOException
    {
        return pushAndStart(endpoint, project, workflow, ImmutableMap.of());
    }

    public static Id pushAndStart(String endpoint, Path project, String workflow, Map<String, String> params)
            throws IOException
    {
        String projectName = project.getFileName().toString();

        // Push the project
        pushProject(endpoint, project, projectName);

        return startWorkflow(endpoint, projectName, workflow, params);
    }

    public static Id startWorkflow(String endpoint, String projectName, String workflow)
    {
        return startWorkflow(endpoint, projectName, workflow, ImmutableMap.of());
    }

    public static Id startWorkflow(String endpoint, String projectName, String workflow, Map<String, String> params)
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

    public static Id pushProject(String endpoint, Path project)
    {
        String projectName = project.getFileName().toString();
        return pushProject(endpoint, project, projectName);
    }

    public static Id pushProject(String endpoint, Path project, String projectName)
    {
        return pushProject(endpoint, project, projectName, asList());
    }

    public static Id pushProject(String endpoint, Path project, String projectName, List<String> additionalArgs)
    {
        List<String> command = new ArrayList<>();
        command.addAll(asList(
                "push",
                "--project", project.toString(),
                projectName,
                "-c", "/dev/null",
                "-e", endpoint));
        command.addAll(additionalArgs);
        CommandStatus pushStatus = main(command);
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        Matcher matcher = PROJECT_ID_PATTERN.matcher(pushStatus.outUtf8());
        boolean found = matcher.find();
        assertThat(found, is(true));
        return Id.of(matcher.group(1));
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

    public static CommandStatus runWorkflow(TemporaryFolder folder, String resource, Map<String, String> params)
            throws IOException
    {
        return runWorkflow(folder, resource, params, ImmutableMap.of());
    }

    public static CommandStatus runWorkflow(TemporaryFolder folder, String resource, Map<String, String> params, Map<String, String> config)
            throws IOException
    {
        return runWorkflow(folder, resource, params, config, 0);
    }

    public static CommandStatus runWorkflow(TemporaryFolder folder, String resource, Map<String, String> params, Map<String, String> config, int expectedStatus)
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
            return status;
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
        try {
            MockWebServer server = new MockWebServer();
            server.setDispatcher(new NopDispatcher());
            if (https) {
                HandshakeCertificates handshakeCertificates = localhost();
                SSLSocketFactory socketFactory = handshakeCertificates.sslSocketFactory();
                server.useHttps(socketFactory, false);

            }
            server.start(0);
            return server;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
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
    public static HttpProxyServer startRequestFailingProxy(int failures, ConcurrentMap<String, List<FullHttpRequest>> requests, HttpResponseStatus error)
    {
        return startRequestFailingProxy(failures, requests, error, (req, reqCount) -> Optional.absent());
    }

    /**
     * Starts a proxy that fails all requests except every {@code failures}'th request per unique (method, uri) pair.
     */
    public static HttpProxyServer startRequestFailingProxy(int defaultFailures, ConcurrentMap<String, List<FullHttpRequest>> requests, HttpResponseStatus error,
            BiFunction<FullHttpRequest, Integer, Optional<Boolean>> customFailDecider)
    {
        return startRequestFailingProxy(request -> {
            String key = request.getMethod() + " " + request.getUri();
            List<FullHttpRequest> keyedRequests = requests.computeIfAbsent(key, k -> new ArrayList<>());
            int n;
            synchronized (keyedRequests) {
                keyedRequests.add(request.copy());
                n = keyedRequests.size();
            }
            boolean fail = customFailDecider.apply(request, n).or(() -> n % defaultFailures != 0);
            if (fail) {
                return Optional.of(error);
            }
            else {
                return Optional.absent();
            }
        });
    }

    public static HttpProxyServer startRequestFailingProxy(final Function<FullHttpRequest, Optional<HttpResponseStatus>> failer)
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
                                Optional<HttpResponseStatus> error = failer.apply(fullHttpRequest);
                                if (error.isPresent()) {
                                    logger.info("Simulating {} for request: {} {}", error, fullHttpRequest.getMethod(), fullHttpRequest.getUri());
                                    HttpResponse response = new DefaultFullHttpResponse(httpRequest.getProtocolVersion(), error.get());
                                    response.headers().set(CONNECTION, CLOSE);
                                    return response;
                                }
                                else {
                                    logger.info("Passing request: {} {}", fullHttpRequest.getMethod(), fullHttpRequest.getUri());
                                    return null;
                                }
                            }
                        };
                    }
                }).start();
    }

    public static void s3Put(AmazonS3 s3, String bucket, String key, String resource)
            throws IOException
    {
        logger.info("put {} -> s3://{}/{}", resource, bucket, key);
        URL resourceUrl = Resources.getResource(resource);
        byte[] bytes = Resources.toByteArray(resourceUrl);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        s3.putObject(bucket, key, new ByteArrayInputStream(bytes), metadata);
    }

    public static void s3DeleteRecursively(AmazonS3 s3, String bucket, String prefix)
            throws Exception
    {
        ListObjectsRequest request = new ListObjectsRequest()
                .withBucketName(bucket)
                .withPrefix(prefix);

        while (true) {
            ObjectListing listing = s3.listObjects(request);
            String[] keys = listing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).toArray(String[]::new);
            for (String key : keys) {
                logger.info("delete s3://{}/{}", bucket, key);
            }
            retryExecutor()
                    .retryIf(e -> e instanceof AmazonServiceException)
                    .run(() -> s3.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(keys)));
            if (listing.getNextMarker() == null) {
                break;
            }
        }
    }

    public static void assertCommandStatus(CommandStatus status)
    {
        assertCommandStatus(status, Optional.absent());
    }

    public static void assertCommandStatus(CommandStatus status, Optional<String> partOfErrorMessage)
    {
        if (partOfErrorMessage.isPresent()) {
            // Failed
            assertThat(status.code(), is(1));
            assertThat(status.errUtf8(), is(containsString(partOfErrorMessage.get())));
        }
        else {
            // Finished successfully
            assertThat(status.errUtf8(), status.code(), is(0));
        }
    }
}
