package acceptance;

import com.google.common.base.Optional;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionAttempt;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;

import static acceptance.TestUtils.copyResource;
import static acceptance.TestUtils.getAttemptId;
import static acceptance.TestUtils.getSessionId;
import static acceptance.TestUtils.main;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class BackfillIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private Path projectDir;
    private DigdagClient client;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();

        client = DigdagClient.builder()
                .host(server.host())
                .port(server.port())
                .build();
    }

    @Test
    public void initPushBackfill()
            throws Exception
    {
        // Create new project
        {
            CommandStatus cmd = main("init",
                    "-c", config.toString(),
                    projectDir.toString());
            assertThat(cmd.code(), is(0));
        }

        copyResource("acceptance/backfill/backfill.dig", projectDir.resolve("backfill.dig"));

        // Push
        {
            CommandStatus cmd = main("push",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "--project", projectDir.toString(),
                    "backfill-test");
            assertThat(cmd.errUtf8(), cmd.code(), is(0));
        }

        copyResource("acceptance/backfill/backfill.dig", projectDir.resolve("backfill.dig"));

        // Backfill the workflow
        {
            CommandStatus cmd = main("backfill",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "backfill-test", "backfill",
                    "--from", "2016-01-01",
                    "--count", "2");
            assertThat(cmd.errUtf8(), cmd.code(), is(0));
        }

        // Verify that 2 sessions are started
        List<RestSession> sessions = client.getSessions();
        assertThat(sessions.size(), is(2));

        // sessions API return results in reversed order

        RestSession session1 = sessions.get(1);
        assertThat(session1.getSessionTime(), is(OffsetDateTime.parse("2016-01-01T00:00:00+09:00")));

        RestSession session2 = sessions.get(0);
        assertThat(session2.getSessionTime(), is(OffsetDateTime.parse("2016-01-02T00:00:00+09:00")));
    }
}
