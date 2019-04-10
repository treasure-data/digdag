package acceptance;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;

public class CliShowProjectsIT
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
        config = folder.newFile().toPath();
    }

    @Test
    public void showProjects()
            throws Exception
    {

        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/basic.dig", projectDir.resolve("basic.dig"));

        // digdag push first project. (named: foo)
        {
            CommandStatus pushStatus = main(
                    "push",
                    "--project", projectDir.toString(),
                    "foo",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        // digdag push second project. (named: bar)
        {
            CommandStatus pushStatus = main(
                    "push",
                    "--project", projectDir.toString(),
                    "bar",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        CommandStatus showProjectStatus = main("projects",
                "-c", config.toString(),
                "-e", server.endpoint());

        assertThat(showProjectStatus.errUtf8(), showProjectStatus.code(), is(0));
        assertThat(showProjectStatus.outUtf8(), containsString(" name: foo"));
        assertThat(showProjectStatus.outUtf8(), containsString(" name: bar"));
    }

    @Test
    public void showProjectWithProjectName()
            throws Exception
    {

        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/basic.dig", projectDir.resolve("basic.dig"));

        // digdag push first project. (named: foo)
        {
            CommandStatus pushStatus = main(
                    "push",
                    "--project", projectDir.toString(),
                    "foo",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        // digdag push second project. (named: bar)
        {
            CommandStatus pushStatus = main(
                    "push",
                    "--project", projectDir.toString(),
                    "bar",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        CommandStatus showProjectStatus = main("projects","foo",
                "-c", config.toString(),
                "-e", server.endpoint());

        assertThat(showProjectStatus.errUtf8(), showProjectStatus.code(), is(0));
        assertThat(showProjectStatus.outUtf8(), containsString(" name: foo"));
        // This assert expect `digdag projects foo` doesn't show project `bar` detail.
        assertThat(showProjectStatus.outUtf8(), not(containsString(" name: bar")));
    }
    @Test
    public void showProjectWithNonRegisteredProject()
            throws Exception
    {

        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/basic.dig", projectDir.resolve("basic.dig"));

        // digdag push first project. (named: foo)
        {
            CommandStatus pushStatus = main(
                    "push",
                    "--project", projectDir.toString(),
                    "foo",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        // digdag push second project. (named: bar)
        {
            CommandStatus pushStatus = main(
                    "push",
                    "--project", projectDir.toString(),
                    "bar",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
        }

        CommandStatus showProjectStatus = main("projects","not_exist_project",
                "-c", config.toString(),
                "-e", server.endpoint());

        assertThat(showProjectStatus.errUtf8(), showProjectStatus.code(), is(1));
        assertThat(showProjectStatus.errUtf8(), containsString("does not exist."));
    }

}
