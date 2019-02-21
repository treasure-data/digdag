package io.digdag.server.rs;

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
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;

public class ProjectResourceTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path projectDir;

    @Before
    public void setUp()
            throws Exception
    {
        String subDir = "project_resource_test";
        folder.newFolder(subDir);
        projectDir = folder.getRoot().toPath().resolve(subDir);
        copyResource("acceptance/basic.dig", projectDir.resolve("basic.dig"));
    }

    @Test
    public void uploadProject()
            throws Exception
    {
        TemporaryDigdagServer server = TemporaryDigdagServer.builder()
                .build();
        server.start();

        CommandStatus pushStatus = main("push", "--project", projectDir.toString(), "foobar", "-e", server.endpoint());

        assertThat(pushStatus.code(),is(0));
        server.close();
    }

    @Test
    public void uploadProjectLargerThanLimit()
            throws Exception
    {
        TemporaryDigdagServer server = TemporaryDigdagServer.builder()
                .configuration("api.max_archive_total_size_limit = 1")
                .build();
        server.start();

        String expectMessage = "Size of the uploaded archive file exceeds limit";

        CommandStatus pushStatus = main("push", "--project", projectDir.toString(), "foobar", "-e", server.endpoint());

        assertThat(pushStatus.errUtf8(), containsString(expectMessage));
        server.close();
    }
}