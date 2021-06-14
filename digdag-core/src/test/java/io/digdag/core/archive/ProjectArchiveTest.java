package io.digdag.core.archive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.digdag.core.repository.WorkflowDefinitionList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

import static io.digdag.client.config.ConfigUtils.newConfig;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ProjectArchiveTest
{
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private Path path(String... fileNames)
    {
        Path p = folder.getRoot().toPath().normalize().toAbsolutePath();
        for (String fileName : fileNames) {
            p = p.resolve(fileName);
        }
        return p;
    }

    private ProjectArchive projectArchive()
    {
        ArchiveMetadata emptyMetadata = ArchiveMetadata.of(
                WorkflowDefinitionList.of(ImmutableList.of()),
                newConfig());
        return new ProjectArchive(path("").normalize().toAbsolutePath(), emptyMetadata);
    }

    @Test
    public void listFilesLookupRecursively()
        throws IOException
    {
        Path d1 = Files.createDirectory(path("d1"));
        Path d2 = Files.createDirectory(path("d1", "d2"));
        Path d3 = Files.createDirectory(path("d1", "d2", "d3"));

        Path f1 = path("d1", "f1");
        Path f2 = path("d1", "d2", "f2");
        Path f3 = path("d1", "d2", "f3");
        Path f4 = path("d1", "d2", "d3", "f4");
        Path f5 = path("d1", "d2", "d3", "f5");

        for (Path p : ImmutableList.of(f1, f2, f3, f4, f5)) {
            Files.write(p, "".getBytes(UTF_8));
        }

        Map<String, Path> files = new HashMap<>();
        projectArchive().listFiles(files::put);

        // keys are normalized path names
        ImmutableMap.Builder<String, Path> expected = ImmutableMap.builder();
        expected.put("d1", d1);
        expected.put("d1/d2", d2);
        expected.put("d1/d2/d3", d3);
        expected.put("d1/f1", f1);
        expected.put("d1/d2/f2", f2);
        expected.put("d1/d2/f3", f3);
        expected.put("d1/d2/d3/f4", f4);
        expected.put("d1/d2/d3/f5", f5);

        assertThat(files, is(expected.build()));
    }

    @Test
    public void listFilesExcludeDotFiles()
        throws IOException
    {
        Path d1 = Files.createDirectory(path("d1"));
        Path d2 = Files.createDirectory(path("d1", "d2"));
        Path d3 = Files.createDirectory(path("d1", "d2", "d3"));

        Path f1 = path("d1", ".f1");
        Path f2 = path("d1", "d2", ".f3");
        Path f3 = path("d1", "d2", "d3", ".f3");

        for (Path p : ImmutableList.of(f1, f2, f3)) {
            Files.write(p, "".getBytes(UTF_8));
        }

        Map<String, Path> files = new HashMap<>();
        projectArchive().listFiles(files::put);

        ImmutableMap.Builder<String, Path> expected = ImmutableMap.builder();
        expected.put("d1", d1);
        expected.put("d1/d2", d2);
        expected.put("d1/d2/d3", d3);

        assertThat(files, is(expected.build()));
    }

    @Test
    public void rejectsOutsideOfProjectPath()
        throws IOException
    {
        ProjectArchive archive = projectArchive();

        exception.expectMessage(containsString(" is outside of project directory "));
        exception.expect(IllegalArgumentException.class);

        archive.pathToResourceName(Paths.get("" + File.separatorChar + "root"));
    }
}
