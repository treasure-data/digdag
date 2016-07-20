package utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import io.digdag.cli.Main;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Version;
import org.apache.commons.io.FileUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static io.digdag.core.Version.buildVersion;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TestUtils
{
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

    public static CommandStatus main(String... args)
    {
        return main(buildVersion(), args);
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
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ByteArrayOutputStream err = new ByteArrayOutputStream();
        return main(localVersion, args, out, err);
    }

    public static CommandStatus main(Version localVersion, Collection<String> args, ByteArrayOutputStream out, ByteArrayOutputStream err)
    {
        final int code;
        try (
                PrintStream outp = new PrintStream(out, true, "UTF-8");
                PrintStream errp = new PrintStream(err, true, "UTF-8");
        ) {
            code = new Main(localVersion, outp, errp).cli(args.stream().toArray(String[]::new));
        }
        catch (RuntimeException | UnsupportedEncodingException e) {
            e.printStackTrace();
            Assert.fail();
            throw Throwables.propagate(e);
        }
        return CommandStatus.of(code, out.toByteArray(), err.toByteArray());
    }

    public static void copyResource(String resource, Path dest)
            throws IOException
    {
        try (InputStream input = Resources.getResource(resource).openStream()) {
            Files.copy(input, dest, REPLACE_EXISTING);
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

    public static String getAttemptLogs(DigdagClient client, long attemptId)
            throws IOException
    {
        List<RestLogFileHandle> handles = client.getLogFileHandlesOfAttempt(attemptId);
        StringBuilder logs = new StringBuilder();
        for (RestLogFileHandle handle : handles) {
            try (InputStream s = new GZIPInputStream(client.getLogFile(attemptId, handle))) {
                logs.append(new String(ByteStreams.toByteArray(s), UTF_8));
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
            return attemptsStatus.outUtf8().contains("status: error");
        };
    }

    public static Callable<Boolean> attemptSuccess(String endpoint, long attemptId)
    {
        return () -> {
            CommandStatus attemptsStatus = attempts(endpoint, attemptId);
            return attemptsStatus.outUtf8().contains("status: success");
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
        CommandStatus pushStatus = main("push",
                "--project", project.toString(),
                projectName,
                "-c", "/dev/null",
                "-e", endpoint,
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

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

    public static void addWorkflow(Path project, String resource)
            throws IOException
    {
        Path workflow = Paths.get(resource);
        addWorkflow(project, resource, workflow.getFileName().toString());
    }

    public static void addWorkflow(Path project, String resource, String workflowName)
            throws IOException
    {
        copyResource(resource, project.resolve(workflowName));
    }

    public static void runWorkflow(String resource, ImmutableMap<String, String> params)
            throws IOException
    {
        Path workflow = Paths.get(resource);
        Path tempdir = Files.createTempDirectory("digdag-test");
        Path file = tempdir.resolve(workflow.getFileName());
        List<String> runCommand = new ArrayList<>(asList("run",
                "-c", "/dev/null",
                "-o", tempdir.toString(),
                "--project", tempdir.toString(),
                workflow.getFileName().toString()));
        params.forEach((k, v) -> runCommand.addAll(asList("-p", k + "=" + v)));
        try {
            copyResource(resource, file);
            CommandStatus status = main(runCommand);
            assertThat(status.errUtf8(), status.code(), is(0));
        }
        finally {
            FileUtils.deleteQuietly(tempdir.toFile());
        }
    }

    public static ObjectMapper objectMapper()
    {
        return OBJECT_MAPPER;
    }

    public static ConfigFactory configFactory()
    {
        return new ConfigFactory(objectMapper());
    }
}
