package acceptance;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static utils.TestUtils.main;

import static io.digdag.core.archive.ProjectArchive.WORKFLOW_FILE_SUFFIX;

public class InitIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path tempdir;

    @Before
    public void setUp()
            throws Exception
    {
        tempdir = folder.getRoot().toPath().toAbsolutePath();
    }

    @Test
    public void exampleTypeEcho()
    {
        String type = "echo";
        String projectName = type + "_test";
        Path projectDir = tempdir.resolve(projectName);

        // Create new project
        CommandStatus initStatus = main("init",
                "-t", type,
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        File[] files = projectDir.toFile().listFiles();
        assertThat(files.length, is(2));
        Arrays.stream(files).allMatch(
                (f) ->
                        (f.getPath().equals(projectName + WORKFLOW_FILE_SUFFIX) && f.isFile())
                                || (f.getPath().equals(".digdag") && f.isDirectory())
        );
    }

    @Test
    public void exampleTypeSh()
    {
        String type = "sh";
        String projectName = type + "_test";
        Path projectDir = tempdir.resolve(projectName);

        // Create new project
        CommandStatus initStatus = main("init",
                "-t", type,
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        File[] files = projectDir.toFile().listFiles();
        assertThat(files.length, is(3));
        Arrays.stream(files).allMatch(
                (f) ->
                        (f.getPath().equals(projectName + WORKFLOW_FILE_SUFFIX) && f.isFile())
                                || (f.getPath().equals(".digdag") && f.isDirectory())
                                || (f.getPath().equals("scripts") && f.isDirectory())
        );
        File[] scripts = projectDir.resolve("scripts").toFile().listFiles();
        assertThat(scripts.length, is(1));
        Arrays.stream(scripts).allMatch(
                (f) ->
                        (f.getPath().equals("myscript.sh") && f.isFile())
        );
    }

    @Test
    public void exampleTypeRuby()
    {
        String type = "ruby";
        String projectName = type + "_test";
        Path projectDir = tempdir.resolve(projectName);

        // Create new project
        CommandStatus initStatus = main("init",
                "-t", type,
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        File[] files = projectDir.toFile().listFiles();
        assertThat(files.length, is(3));
        Arrays.stream(files).allMatch(
                (f) ->
                        (f.getPath().equals(projectName + WORKFLOW_FILE_SUFFIX) && f.isFile())
                                || (f.getPath().equals(".digdag") && f.isDirectory())
                                || (f.getPath().equals("scripts") && f.isDirectory())
        );
        File[] scripts = projectDir.resolve("scripts").toFile().listFiles();
        assertThat(scripts.length, is(1));
        Arrays.stream(scripts).allMatch(
                (f) ->
                        (f.getPath().equals("myclass.rb") && f.isFile())
        );
    }

    @Test
    public void exampleTypePython()
    {
        String type = "python";
        String projectName = type + "_test";
        Path projectDir = tempdir.resolve(projectName);

        // Create new project
        CommandStatus initStatus = main("init",
                "-t", type,
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        File[] files = projectDir.toFile().listFiles();
        assertThat(files.length, is(3));
        Arrays.stream(files).allMatch(
                (f) ->
                        (f.getPath().equals(projectName + WORKFLOW_FILE_SUFFIX) && f.isFile())
                                || (f.getPath().equals(".digdag") && f.isDirectory())
                                || (f.getPath().equals("scripts") && f.isDirectory())
        );
        File[] scripts = projectDir.resolve("scripts").toFile().listFiles();
        assertThat(scripts.length, is(2));
        Arrays.stream(scripts).allMatch(
                (f) ->
                        (f.getPath().equals("__init__.py") && f.isFile())
                                || (f.getPath().equals("myclass.py") && f.isFile())
        );
    }

    @Test
    public void exampleTypeTd()
    {
        String type = "td";
        String projectName = type + "_test";
        Path projectDir = tempdir.resolve(projectName);

        // Create new project
        CommandStatus initStatus = main("init",
                "-t", type,
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        File[] files = projectDir.toFile().listFiles();
        assertThat(files.length, is(3));
        Arrays.stream(files).allMatch(
                (f) ->
                        (f.getPath().equals(projectName + WORKFLOW_FILE_SUFFIX) && f.isFile())
                                || (f.getPath().equals(".digdag") && f.isDirectory())
                                || (f.getPath().equals("queries") && f.isDirectory())
        );
        File[] scripts = projectDir.resolve("queries").toFile().listFiles();
        assertThat(scripts.length, is(1));
        Arrays.stream(scripts).allMatch(
                (f) ->
                        (f.getPath().equals("query.sql") && f.isFile())
        );
    }

    @Test
    public void exampleTypePostgreSQL()
    {
        String type = "postgresql";
        String projectName = type + "_test";
        Path projectDir = tempdir.resolve(projectName);

        // Create new project
        CommandStatus initStatus = main("init",
                "-t", type,
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        File[] files = projectDir.toFile().listFiles();
        assertThat(files.length, is(3));
        Arrays.stream(files).allMatch(
                (f) ->
                        (f.getPath().equals(projectName + WORKFLOW_FILE_SUFFIX) && f.isFile())
                                || (f.getPath().equals(".digdag") && f.isDirectory())
                                || (f.getPath().equals("queries") && f.isDirectory())
        );
        File[] scripts = projectDir.resolve("queries").toFile().listFiles();
        assertThat(scripts.length, is(3));
        Arrays.stream(scripts).allMatch(
                (f) ->
                        (f.getPath().equals("create_src_table.sql") && f.isFile())
                                || (f.getPath().equals("insert_data_to_src_table.sql") && f.isFile())
                                || (f.getPath().equals("summarize_src_table.sql") && f.isFile())
        );
    }
}
