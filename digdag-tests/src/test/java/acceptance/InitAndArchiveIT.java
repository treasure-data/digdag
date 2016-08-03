package acceptance;

import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static utils.TestUtils.main;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class InitAndArchiveIT
{
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

        Map<String, byte[]> entries = readEntries();

        // TODO (dano): add more exhaustive verification of archive contents
        assertThat(entries, hasKey("foobar.dig"));
    }

    private Map<String, byte[]> readEntries()
            throws IOException
    {
        Map<String, byte[]> entries = new HashMap<>();
        try (TarArchiveInputStream s = new TarArchiveInputStream(
                new GzipCompressorInputStream(Files.newInputStream(archive)))) {
            while (true) {
                TarArchiveEntry entry = s.getNextTarEntry();
                if (entry == null) {
                    break;
                }
                entries.put(entry.getName(), ByteStreams.toByteArray(s));
            }
        }
        return entries;
    }
}
