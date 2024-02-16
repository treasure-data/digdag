package io.digdag.core.archive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableSet;

import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.config.YamlConfigLoader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import static io.digdag.client.config.ConfigUtils.configFactory;
import static io.digdag.client.config.ConfigUtils.newConfig;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ProjectArchiveLoaderTest
{
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private Path path(String... fileNames)
    {
        Path p = folder.getRoot().toPath().normalize().toAbsolutePath();
        for (String fileName : fileNames) {
            p = p.resolve(fileName);
        }
        return p;
    }

    private ProjectArchive loadProject()
        throws IOException
    {
        ProjectArchiveLoader loader = new ProjectArchiveLoader(
                new ConfigLoaderManager(configFactory, new YamlConfigLoader()));
        System.out.println("loadProject ーーーーーーーーーーーーーーーーーー");
        System.out.println("ここは通っていない");
        return loader.load(
                path(),
                WorkflowResourceMatcher.defaultMatcher(),
                newConfig());
    }

    @Test
    public void buildsWorkflowNameFromFileName()
        throws IOException
    {
        Files.write(path("example.dig"), "+a: {echo>: a}".getBytes(UTF_8));

        ProjectArchive archive = loadProject();

        Set<String> workflowNames = ImmutableSet.copyOf(
                archive.getArchiveMetadata().getWorkflowList().get()
                .stream().map(def -> def.getName()).iterator());

        assertThat(workflowNames, is(ImmutableSet.of("example")));
    }

    @Test
    public void ignoresWorkflowsInSubdirectories()
        throws IOException
    {
        Files.createDirectories(path("d1", "d2"));
        Files.write(path("d1", "sub1.dig"), "+sub: {echo>: sub}".getBytes(UTF_8));
        Files.write(path("d1", "d2", "sub2.dig"), "+sub: {echo>: sub}".getBytes(UTF_8));

        ProjectArchive archive = loadProject();

        Set<String> workflowNames = ImmutableSet.copyOf(
                archive.getArchiveMetadata().getWorkflowList().get()
                .stream().map(def -> def.getName()).iterator());

        assertThat(workflowNames, is(ImmutableSet.of()));
    }

    @Test
    public void ignoresDirectories()
        throws IOException
    {
        Files.createDirectories(path("example.dig"));

        ProjectArchive archive = loadProject();

        Set<String> workflowNames = ImmutableSet.copyOf(
                archive.getArchiveMetadata().getWorkflowList().get()
                .stream().map(def -> def.getName()).iterator());

        assertThat(workflowNames, is(ImmutableSet.of()));
    }
}
