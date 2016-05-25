package acceptance;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import io.digdag.cli.Main;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.core.Version;
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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static io.digdag.core.Version.buildVersion;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

class TestUtils
{
    /**
     * attempt id pattern emitted by `digdag start`
     */
    static final Pattern START_ATTEMPT_ID_PATTERN = Pattern.compile("\\s*id:\\s*(\\d+)\\s*");

    /**
     * attempt id pattern emitted by `digdag attempts`
     */
    static final Pattern ATTEMPTS_ATTEMPT_ID_PATTERN = Pattern.compile("\\s*attempt id:\\s*(\\d+)\\s*");

    static CommandStatus main(String... args)
    {
        return main(buildVersion(), args);
    }

    static CommandStatus main(Version localVersion, String... args) {
        return main(localVersion, asList(args));
    }

    static CommandStatus main(Version localVersion, Collection<String> args)
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ByteArrayOutputStream err = new ByteArrayOutputStream();
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
        //System.out.println(new String(out.toByteArray()));
        //System.err.println(new String(err.toByteArray()));
        return CommandStatus.of(code, out.toByteArray(), err.toByteArray());
    }

    static void copyResource(String resource, Path dest)
            throws IOException
    {
        try (InputStream input = Resources.getResource(resource).openStream()) {
            Files.copy(input, dest, REPLACE_EXISTING);
        }
    }

    static void fakeHome(String home, Action a)
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

    static int findFreePort()
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    static long getStartAttemptId(CommandStatus startStatus)
    {
        Matcher matcher = START_ATTEMPT_ID_PATTERN.matcher(startStatus.outUtf8());
        assertThat(matcher.find(), is(true));
        return Long.parseLong(matcher.group(1));
    }

    static long getAttemptsAttemptId(CommandStatus attemptsStatus)
    {
        Matcher attemptsAttemptIdMatcher = ATTEMPTS_ATTEMPT_ID_PATTERN.matcher(attemptsStatus.outUtf8());
        assertThat(attemptsAttemptIdMatcher.find(), is(true));
        return Long.parseLong(attemptsAttemptIdMatcher.group(1));
    }

    static String getAttemptLogs(DigdagClient client, long attemptId)
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

    static <T> org.hamcrest.Matcher<T> validUuid()
    {
        return new BaseMatcher<T>() {
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
}
