package acceptance;

import com.google.common.io.ByteStreams;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
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
import static org.hamcrest.Matchers.notNullValue;
import static utils.TestUtils.copyResource;
import static utils.TestUtils.main;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CliArchiveIT
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
    public void archiveProject()
            throws Exception
    {
        // Create new project
        CommandStatus initStatus = main("init",
                "-c", config.toString(),
                projectDir.toString());
        assertThat(initStatus.code(), is(0));

        copyResource("acceptance/basic.dig", projectDir.resolve("basic.dig"));

        // 1. archive project
        {
            CommandStatus archiveStatus = main(
                    "archive",
                    "--project", projectDir.toString(),
                    "--output", "test_archive.tar.gz",
                    "-c", config.toString()
            );
            assertThat(archiveStatus.errUtf8(), archiveStatus.code(), is(0));
        }

        // 2. then upload it.
        {
            CommandStatus uploadStatus = main(
                    "upload",
                    "test_archive.tar.gz",
                    "test_archive_proj",
                    "-c", config.toString(),
                    "-e", server.endpoint());
            assertThat(uploadStatus.errUtf8(), uploadStatus.code(), is(0));
        }
    }

    @Test
    public void rejectSymlinksPointingParents()
            throws Exception
    {
        Path sub = Files.createDirectories(projectDir.resolve("sub"));

        // sub/from1 points ../to1 => should success
        Files.createSymbolicLink(sub.resolve("from1"), Paths.get("..").resolve("to1"));
        CommandStatus status1 = main(
                "archive",
                "--project", projectDir.toString(),
                "--output", "test_archive.tar.gz",
                "-c", config.toString()
        );
        assertThat(status1.errUtf8(), status1.code(), is(0));

        // sub/from1 points ../../to1 => should fail
        Files.createSymbolicLink(sub.resolve("from2"), Paths.get("..").resolve("..").resolve("to2"));
        CommandStatus status2 = main(
                "archive",
                "--project", projectDir.toString(),
                "--output", "test_archive.tar.gz",
                "-c", config.toString()
        );
        assertThat(status2.errUtf8(), status2.code(), is(1));
        assertThat(status2.errUtf8(), containsString("is outside of project directory"));
    }

    @Test
    public void copyOutsideSymlinksCopiesFile()
            throws Exception
    {
        Files.createDirectories(projectDir);
        Path archivePath = projectDir.resolve("..").resolve("test_archive.tar.gz");

        // ../to1 => "text1"
        Path to1 = projectDir.resolve("..").resolve("to1");
        Files.write(to1, "text1".getBytes(UTF_8));

        // sub/from1 points ../../to1
        Path sub = Files.createDirectories(projectDir.resolve("sub"));
        Files.createSymbolicLink(sub.resolve("from1"), Paths.get("..").resolve("..").resolve("to1"));

        // archive with --copy-symlink
        CommandStatus status1 = main(
                "archive",
                "--project", projectDir.toString(),
                "--output", archivePath.toString(),
                "--copy-outside-symlinks",
                "-c", config.toString()
        );
        assertThat(status1.errUtf8(), status1.code(), is(0));

        boolean found = false;
        try (TarArchiveInputStream s = new TarArchiveInputStream(
                new GzipCompressorInputStream(Files.newInputStream(archivePath)))) {
            while (true) {
                TarArchiveEntry entry = s.getNextTarEntry();
                if (entry == null) {
                    break;
                }

                if (entry.getName().equals("sub1/from1")) {
                    found = true;
                    String contents = new String(ByteStreams.toByteArray(s), UTF_8);
                    assertThat(contents, is("text1"));
                }
            }
        }
        assertThat(found, is(false));
    }

    @Test
    public void copyOutsideSymlinksCopiesDirectory()
            throws Exception
    {
        Files.createDirectories(projectDir);
        Path archivePath = projectDir.resolve("..").resolve("test_archive.tar.gz");

        // ../to1/file => "text1"
        Path to1 = projectDir.resolve("..").resolve("to1");
        Files.createDirectories(to1);
        Files.write(to1.resolve("file"), "text1".getBytes(UTF_8));

        // sub/from1 points ../../to1/
        Path sub = Files.createDirectories(projectDir.resolve("sub"));
        Files.createSymbolicLink(sub.resolve("from1"), Paths.get("..").resolve("..").resolve("to1"));

        // archive with --copy-symlink
        CommandStatus status1 = main(
                "archive",
                "--project", projectDir.toString(),
                "--output", archivePath.toString(),
                "--copy-outside-symlinks",
                "-c", config.toString()
        );
        assertThat(status1.errUtf8(), status1.code(), is(0));

        boolean found = false;
        try (TarArchiveInputStream s = new TarArchiveInputStream(
                new GzipCompressorInputStream(Files.newInputStream(archivePath)))) {
            while (true) {
                TarArchiveEntry entry = s.getNextTarEntry();
                if (entry == null) {
                    break;
                }

                if (entry.getName().equals("sub1/from1/file")) {
                    found = true;
                    String contents = new String(ByteStreams.toByteArray(s), UTF_8);
                    assertThat(contents, is("text1"));
                }
            }
        }
        assertThat(found, is(false));
    }
}
