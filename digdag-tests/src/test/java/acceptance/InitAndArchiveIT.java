package acceptance;

import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import utils.CommandStatus;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static utils.TestUtils.main;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assume.assumeTrue;
import static java.nio.charset.StandardCharsets.UTF_8;

public class InitAndArchiveIT
{
    private interface IoBiConsumer<T, U>
    {
        void accept(T t, U u) throws IOException;
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path project;
    private Path archive;

    @Before
    public void setUp()
            throws Exception
    {
        project = folder.getRoot().toPath().resolve("foobar");
        archive = folder.getRoot().toPath().resolve("digdag.tar.gz");
    }

    @Test
    public void archive()
            throws Exception
    {
        main("init", project.toString());
        main("archive",
                "--project", project.toString(),
                "-o", archive.toString());

        assertThat(Files.exists(archive), is(true));

        Map<String, byte[]> entries = new HashMap<>();
        forEachArchiveEntry((entry, in) -> entries.put(entry.getName(), ByteStreams.toByteArray(in)));

        // TODO (dano): add more exhaustive verification of archive contents
        assertThat(entries, hasKey("foobar.dig"));
    }

    private void forEachArchiveEntry(IoBiConsumer<TarArchiveEntry, InputStream> consumer)
            throws IOException
    {
        try (TarArchiveInputStream s = new TarArchiveInputStream(
                new GzipCompressorInputStream(Files.newInputStream(archive)))) {
            while (true) {
                TarArchiveEntry entry = s.getNextTarEntry();
                if (entry == null) {
                    break;
                }
                consumer.accept(entry, s);
            }
        }
    }

    private void assumeSymlinkSupported()
            throws IOException
    {
        try {
            Files.createSymbolicLink(folder.getRoot().toPath().resolve("target"), folder.getRoot().toPath().resolve("tmp"));
        } catch (UnsupportedOperationException ex) {
            assumeTrue(false);
        }
    }

    @Test
    public void normalizeSymlinksToRelative()
            throws Exception
    {
        assumeSymlinkSupported();

        Path projectDir = Files.createDirectories(project);

        Path subDir = Files.createDirectories(projectDir.resolve("sub1"));
        Path subSubDir = Files.createDirectories(subDir.resolve("sub2"));

        Files.write(projectDir.resolve("proj.file"), "proj".getBytes(UTF_8));
        Files.write(subDir.resolve("sub.file"), "sub".getBytes(UTF_8));
        Files.write(subSubDir.resolve("subsub.file"), "subsub".getBytes(UTF_8));

        Files.createSymbolicLink(projectDir.resolve("sym_proj.file"), Paths.get("proj.file"));
        Files.createSymbolicLink(subDir.resolve("sym_proj.file"), Paths.get("../proj.file"));
        Files.createSymbolicLink(subSubDir.resolve("sym_proj.file"), Paths.get("../../proj.file"));

        Files.createSymbolicLink(projectDir.resolve("sym_sub.file"), Paths.get("sub1/sub.file"));
        Files.createSymbolicLink(subDir.resolve("sym_sub.file"), Paths.get("sub.file"));
        Files.createSymbolicLink(subSubDir.resolve("sym_sub.file"), Paths.get("../sub.file"));

        Files.createSymbolicLink(projectDir.resolve("sym_subsub.file"), Paths.get("sub1/sub2/subsub.file"));
        Files.createSymbolicLink(subDir.resolve("sym_subsub.file"), Paths.get("sub2/subsub.file"));
        Files.createSymbolicLink(subSubDir.resolve("sym_subsub.file"), Paths.get("subsub.file"));

        // project dir name should be removed because the dir path won't be same name on a server
        Files.createSymbolicLink(projectDir.resolve("sym_proj_through_parent.file"), Paths.get("../foobar/proj.file"));
        Files.createSymbolicLink(subDir.resolve("sym_proj_through_parent.file"), Paths.get("../../foobar/proj.file"));
        Files.createSymbolicLink(subSubDir.resolve("sym_proj_through_parent.file"), Paths.get("../../../foobar/proj.file"));

        // absolute path shouldn't be normalized to a relative path because the dir path won't be same name on a server
        Files.createSymbolicLink(projectDir.resolve("sym_proj_absolute.file"), projectDir.resolve("proj.file"));
        Files.createSymbolicLink(subDir.resolve("sym_proj_absolute.file"), projectDir.resolve("proj.file"));
        Files.createSymbolicLink(subSubDir.resolve("sym_proj_absolute.file"), projectDir.resolve("proj.file"));

        Files.createSymbolicLink(projectDir.resolve("sym_sub_absolute.file"), subDir.resolve("sub.file"));
        Files.createSymbolicLink(subDir.resolve("sym_sub_absolute.file"), subDir.resolve("sub.file"));
        Files.createSymbolicLink(subSubDir.resolve("sym_sub_absolute.file"), subDir.resolve("sub.file"));

        Files.createSymbolicLink(projectDir.resolve("sym_subsub_absolute.file"), subSubDir.resolve("subsub.file"));
        Files.createSymbolicLink(subDir.resolve("sym_subsub_absolute.file"), subSubDir.resolve("subsub.file"));
        Files.createSymbolicLink(subSubDir.resolve("sym_subsub_absolute.file"), subSubDir.resolve("subsub.file"));

        CommandStatus command = main("archive",
                "--project", projectDir.toString(),
                "-o", archive.toString());
        assertThat(command.errUtf8(), command.code(), is(0));

        Map<String, TarArchiveEntry> entries = new HashMap<>();
        forEachArchiveEntry((entry, in) -> entries.put(entry.getName(), entry));

        assertThat(entries.get("sym_proj.file").getLinkName(), is("proj.file"));
        assertThat(entries.get("sub1/sym_proj.file").getLinkName(), is("../proj.file"));
        assertThat(entries.get("sub1/sub2/sym_proj.file").getLinkName(), is("../../proj.file"));

        assertThat(entries.get("sym_sub.file").getLinkName(), is("sub1/sub.file"));
        assertThat(entries.get("sub1/sym_sub.file").getLinkName(), is("sub.file"));
        assertThat(entries.get("sub1/sub2/sym_sub.file").getLinkName(), is("../sub.file"));

        assertThat(entries.get("sym_subsub.file").getLinkName(), is("sub1/sub2/subsub.file"));
        assertThat(entries.get("sub1/sym_subsub.file").getLinkName(), is("sub2/subsub.file"));
        assertThat(entries.get("sub1/sub2/sym_subsub.file").getLinkName(), is("subsub.file"));

        // through-parent paths
        assertThat(entries.get("sym_proj_through_parent.file").getLinkName(), is("proj.file"));
        assertThat(entries.get("sub1/sym_proj_through_parent.file").getLinkName(), is("../proj.file"));
        assertThat(entries.get("sub1/sub2/sym_proj_through_parent.file").getLinkName(), is("../../proj.file"));

        // absolute paths
        assertThat(entries.get("sym_proj_absolute.file").getLinkName(), is("proj.file"));
        assertThat(entries.get("sub1/sym_proj_absolute.file").getLinkName(), is("../proj.file"));
        assertThat(entries.get("sub1/sub2/sym_proj_absolute.file").getLinkName(), is("../../proj.file"));

        assertThat(entries.get("sym_sub_absolute.file").getLinkName(), is("sub1/sub.file"));
        assertThat(entries.get("sub1/sym_sub_absolute.file").getLinkName(), is("sub.file"));
        assertThat(entries.get("sub1/sub2/sym_sub_absolute.file").getLinkName(), is("../sub.file"));

        assertThat(entries.get("sym_subsub_absolute.file").getLinkName(), is("sub1/sub2/subsub.file"));
        assertThat(entries.get("sub1/sym_subsub_absolute.file").getLinkName(), is("sub2/subsub.file"));
        assertThat(entries.get("sub1/sub2/sym_subsub_absolute.file").getLinkName(), is("subsub.file"));
    }

    @Test
    public void rejectOuterRelativeSymlinks()
            throws Exception
    {
        assumeSymlinkSupported();

        Path projectDir = Files.createDirectories(project);
        Files.createSymbolicLink(projectDir.resolve("invalid_link.file"), Paths.get("../parent"));

        CommandStatus command = main("archive",
                "--project", projectDir.toString(),
                "-o", archive.toString());
        assertThat(command.errUtf8(), command.code(), is(1));
    }

    @Test
    public void rejectOuterAbsoluteSymlinks()
            throws Exception
    {
        assumeSymlinkSupported();

        Path projectDir = Files.createDirectories(project);
        Files.createSymbolicLink(projectDir.resolve("invalid_link.file"), projectDir.resolve("../sibling"));

        CommandStatus command = main("archive",
                "--project", projectDir.toString(),
                "-o", archive.toString());
        assertThat(command.errUtf8(), command.code(), is(1));
    }
}
