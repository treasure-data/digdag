package acceptance;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;

public class SchedulerIT
{
    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.builder().build();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    @Test
    public void testScheduler()
            throws Exception
    {
        copyResource("acceptance/scheduler/simple.dig", root().resolve("test.dig"));
        CommandStatus status = main("scheduler", "--project", root().toString());
        assertThat(status.errUtf8(), status.code(), is(0));

        for (int i = 0; i < 90; i++) {
            if (Files.exists(root().resolve("foo.out"))) {
                assertTrue(true);
                return;
            }
            TimeUnit.SECONDS.sleep(2);
        }
        assertTrue(false);
    }

    @Test
    public void testInvalidScheduler()
            throws Exception
    {
        try {
            copyResource("acceptance/scheduler/invalid.dig", root().resolve("test.dig"));
            main("scheduler", "--project", root().toString());
            fail();
        }
        catch (Throwable ie) {
            assertThat(ie.getCause().getCause().toString(), Matchers.containsString("Parameter 'skip_on_over_time' is not used at schedule. > Did you mean '[skip_on_overtime]'"));
        }
    }

    @Test
    public void testInvalidOpScheduler()
            throws Exception
    {
        try {
            copyResource("acceptance/scheduler/invalid_op.dig", root().resolve("test.dig"));
            main("scheduler", "--project", root().toString());
            fail();
        }
        catch (Throwable ie) {
            assertThat(ie.getCause().getCause().toString(), Matchers.containsString("Parameter 'wrong>' is not used at schedule."));
        }
    }

    @Test
    public void testUnknownTypeScheduler()
            throws Exception
    {
        try {
            copyResource("acceptance/scheduler/unknown_type.dig", root().resolve("test.dig"));
            main("scheduler", "--project", root().toString());
            fail();
        }
        catch (Throwable ie) {
            assertThat(ie.getCause().getCause().toString(), Matchers.containsString("Unknown scheduler type: no_such_scheduler"));
        }
    }

    @Test
    public void verify400UnusedKeys()
            throws Exception
    {
        OkHttpClient client = new OkHttpClient();
        Path testPath = root().resolve("test.dig");
        copyResource("acceptance/scheduler/invalid.dig", testPath);
        Response response = client.newCall(new Request.Builder()
                .url(server.endpoint() + "/api/projects?project=test&revision=test")
                .method("PUT", RequestBody.create(MediaType.parse("application/gzip"), new File(testPath.toString())))
                .build()).execute();
        assertThat(response.code(), is(400));
    }

    @Test
    public void verify400UnknownScheduler()
            throws Exception
    {
        OkHttpClient client = new OkHttpClient();
        Path testPath = root().resolve("test.dig");
        copyResource("acceptance/scheduler/unknown_type.dig", testPath);
        Response response = client.newCall(new Request.Builder()
                .url(server.endpoint() + "/api/projects?project=test&revision=test")
                .method("PUT", RequestBody.create(MediaType.parse("application/gzip"), new File(testPath.toString())))
                .build()).execute();
        assertThat(response.code(), is(400));
    }
}
