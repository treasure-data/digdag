package acceptance;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;
import utils.TemporaryDigdagServer;

import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class DownloadIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public TemporaryDigdagServer server = TemporaryDigdagServer.of();

    private Path projectDir;
    private Path config;

    @Before
    public void setUp()
            throws Exception
    {
        projectDir = folder.getRoot().toPath().resolve("foobar");
        config = folder.newFile().toPath();
    }

    @Test
    public void download()
            throws Exception
    {
        pushProject("download_proj", "rev4711");

        // Download the project
        Path downloadDir = projectDir.resolve("extract");
        CommandStatus downloadStatus = main("download",
                "download_proj",
                "-o", downloadDir.toString(),
                "-c", config.toString(),
                "-e", server.endpoint());
        assertThat(downloadStatus.errUtf8(), downloadStatus.code(), is(0));

        assertThat(Files.readAllBytes(downloadDir.resolve("basic.dig")), is(readResource("acceptance/basic.dig")));
        assertThat(Files.readAllBytes(downloadDir.resolve("files").resolve("params.yml")), is(readResource("acceptance/params.dig")));

        assertThat(downloadStatus.outUtf8(), containsString("rev4711"));
    }

    @Test
    public void projectNotFound()
            throws Exception
    {
        Path downloadDir = projectDir.resolve("extract");
        CommandStatus downloadStatus = main("download",
                "download_proj",
                "-o", downloadDir.toString(),
                "-c", config.toString(),
                "-e", server.endpoint());
        assertThat(downloadStatus.errUtf8(), downloadStatus.code(), not(0));
        assertThat(downloadStatus.errUtf8(), containsString("project not found: download_proj"));
    }

    @Test
    public void revisionNotFound()
            throws Exception
    {
        pushProject("download_proj", "rev4711");

        // Download the project with -r option
        Path downloadDir = projectDir.resolve("extract");
        CommandStatus downloadStatus = main("download",
                "download_proj",
                "-o", downloadDir.toString(),
                "-r", "no_such_revision",
                "-c", config.toString(),
                "-e", server.endpoint());
        assertThat(downloadStatus.errUtf8(), downloadStatus.code(), not(0));
        assertThat(downloadStatus.errUtf8(), containsString("Project archive of revision 'no_such_revision' does not exist."));
    }

    private void pushProject(String projectName, String revisionName)
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        Files.createDirectories(projectDir.resolve("files"));

        copyResource("acceptance/basic.dig", projectDir.resolve("basic.dig"));
        copyResource("acceptance/params.dig", projectDir.resolve("files").resolve("params.yml"));

        // Push the project
        CommandStatus pushStatus = main("push",
                "--project", projectDir.toString(),
                projectName,
                "-c", config.toString(),
                "-e", server.endpoint(),
                "-r", revisionName);
        assertThat(pushStatus.errUtf8(), pushStatus.code(), is(0));
    }

    private byte[] readResource(String resource)
        throws IOException
    {
        return Resources.asByteSource(Resources.getResource(resource)).read();
    }
}
