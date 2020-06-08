package acceptance;

import io.digdag.client.api.Id;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.Assert.fail;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.getAttemptId;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class RequireIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path config;
    private Path projectDir;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        Files.createDirectories(projectDir);
        config = folder.newFile().toPath();
    }

    @Test
    public void testRequire()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.errUtf8(), initStatus.code(), is(0));

        Path childOutFile = projectDir.resolve("child.out").toAbsolutePath().normalize();
        prepareForChildWF(childOutFile);
        copyResource("acceptance/require/parent.dig", projectDir.resolve("parent.dig"));

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "require",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        Id attemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "require", "parent",
                    "--session", "now");
            assertThat(startStatus.code(), is(0));
            attemptId = getAttemptId(startStatus);
        }

        // Wait for the attempt to complete
        boolean success = false;
        for (int i = 0; i < 30; i++) {
            CommandStatus attemptsStatus = main("attempts",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            success = attemptsStatus.outUtf8().contains("status: success");
            if (success) {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(success, is(true));

        // Verify that the file created by the child workflow is there
        assertThat(Files.exists(childOutFile), is(true));
    }

    @Test
    public void testRequireFailsWhenDependentFails()
            throws Exception
    {
        copyResource("acceptance/require/parent.dig", projectDir.resolve("parent.dig"));
        copyResource("acceptance/require/fail.dig", projectDir.resolve("child.dig"));

        CommandStatus status = main("run",
                "-c", config.toString(),
                "--project", projectDir.toString(),
                "parent.dig");
        assertThat(status.errUtf8(), status.code(), is(not(0)));

        assertThat(status.errUtf8(), containsString("Dependent workflow failed."));
    }

    @Test
    public void testRequireSucceedsWhenDependentFailsButIgnoreFailureIsSet()
            throws Exception
    {
        copyResource("acceptance/require/parent_ignore_failure.dig", projectDir.resolve("parent.dig"));
        copyResource("acceptance/require/fail.dig", projectDir.resolve("child.dig"));

        CommandStatus status = main("run",
                "-c", config.toString(),
                "--project", projectDir.toString(),
                "parent.dig");
        assertThat(status.errUtf8(), status.code(), is(0));
    }

    @Test
    public void testIgnoreProjectIdParam()
            throws Exception
    {
        // If require> op. does not have 'project_id' param and session start with --param project_id=,
        //  --param project_id should be ignored in require> op.
        // In this test --param project_id= is set in start command.
        // If the session will finish successfully, it means that project_id of --param is ignored.

        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.errUtf8(), initStatus.code(), is(0));

        Path childOutFile = projectDir.resolve("child.out").toAbsolutePath().normalize();
        prepareForChildWF(childOutFile);
        copyResource("acceptance/require/parent.dig", projectDir.resolve("parent.dig"));

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                "require",
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", "4711");
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));

        // Start the workflow
        Id attemptId;
        {
            CommandStatus startStatus = main("start",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    "require", "parent",
                    "--session", "now",
                    "--param", "project_id=-1"
            );
            assertThat(startStatus.code(), is(0));
            attemptId = getAttemptId(startStatus);
        }

        // Wait for the attempt to complete
        boolean success = false;
        for (int i = 0; i < 120; i++) {
            CommandStatus attemptsStatus = main("attempts",
                    "-c", config.toString(),
                    "-e", server.endpoint(),
                    String.valueOf(attemptId));
            success = attemptsStatus.outUtf8().contains("status: success");
            if (success) {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(success, is(true)); // --param project_id=-1 is ignored.
    }

    @Test
    public void testRequireToAnotherProjectById()
            throws Exception {
        testRequireToAnotherProject(true, "parent_by_id", "2020-06-05 00:00:01");
    }

    @Test
    public void testRequireToAnotherProjectByName()
            throws Exception {
        testRequireToAnotherProject(false, "parent_by_name", "2020-06-05 00:00:02");
    }

    /**
     * Test for project_id and project_name parameter
     *
     * @param useProjectId      if true require> use project_id, else require> use project_name
     * @param parentProjectName parent project name
     * @param sessionTime
     * @throws Exception
     */
    private void testRequireToAnotherProject(boolean useProjectId, String parentProjectName, String sessionTime)
            throws Exception {
        final String childProjectName = "child_another";

        // Push child project
        Path childProjectDir = folder.getRoot().toPath().resolve("another_foobar");
        Files.createDirectories(childProjectDir);
        copyResource("acceptance/require/child_another_project.dig", childProjectDir);
        CommandStatus pushChildStatus = main("push",
                "--project", childProjectDir.toString(),
                childProjectName,
                "-c", config.toString(),
                "-e", server.endpoint());
        assertThat(pushChildStatus.errUtf8(), pushChildStatus.code(), is(0));

        // extract child project id
        Matcher m = Pattern.compile(".*\\s+id:\\s+(\\d+).*").matcher(pushChildStatus.outUtf8());
        assertThat(m.find(), is(true));
        String childProjectId = m.group(1);

        // Push parent project with project_id: xxx or project_name: yyy based on useProjectId
        String template = Resources.toString(
                Resources.getResource("acceptance/require/parent_another_project.dig"), UTF_8);
        String content = useProjectId ?
                template.replace("__CHILD_PROJECT__", "project_id: " + childProjectId) :
                template.replace("__CHILD_PROJECT__", "project_name: " + childProjectName);
        Files.write(projectDir.resolve("parent_another_project.dig"), asList(content));
        CommandStatus pushParentStatus = main("push",
                "--project", projectDir.toString(),
                parentProjectName,
                "-c", config.toString(),
                "-e", server.endpoint());
        assertThat(pushParentStatus.errUtf8(), pushParentStatus.code(), is(0));

        CommandStatus startStatus = main("start",
                "-c", config.toString(),
                "-e", server.endpoint(),
                parentProjectName, "parent_another_project",
                "--session", sessionTime
        );
        assertThat(startStatus.errUtf8(), startStatus.code(), is(0));
        checkStatus(Arrays.asList(parentProjectName, "parent_another_project"));
        checkStatus(Arrays.asList(childProjectName, "child_another_project"));
    }

    private void checkStatus(List<String> commands) throws InterruptedException {
        for (int i = 0; i < 120; i++) {
            List<String> args = new ArrayList<String>();
            args.addAll(Arrays.asList("sessions", "-c", config.toString(), "-e", server.endpoint()));
            args.addAll(commands);
            CommandStatus status = main(args);
            assertThat(status.errUtf8(), status.code(), is(0));
            if (status.outUtf8().contains("status: success")) {
                return;
            }
            else if (status.outUtf8().contains("status: error")) {
                fail("attempt failed");
            }
            Thread.sleep(1000);
        }
        fail("attempt not finished");
    }

    /***
     * Replaice __FILE__ to real path then save to project dir
     * @param childOutFile
     * @throws IOException
     */
    private void prepareForChildWF(Path childOutFile)
            throws IOException
    {
        try (InputStream input = Resources.getResource("acceptance/require/child.dig").openStream()) {
            String child = new String(ByteStreams.toByteArray(input), "UTF-8")
                    .replace("__FILE__", childOutFile.toString());
            Files.write(projectDir.resolve("child.dig"), child.getBytes("UTF-8"));
        }
    }
}
