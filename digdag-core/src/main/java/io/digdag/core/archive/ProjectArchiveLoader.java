package io.digdag.core.archive;

import java.io.IOException;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.time.ZoneId;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.DirectoryStream;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinitionList;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.archive.ProjectArchive.PathListing;
import io.digdag.core.archive.ProjectArchive.PathConsumer;

public class ProjectArchiveLoader
{
    private final ConfigLoaderManager configLoader;

    @Inject
    public ProjectArchiveLoader(ConfigLoaderManager configLoader)
    {
        this.configLoader = configLoader;
    }

    public ProjectArchive load(
            List<Path> dagfilePaths,
            Config overwriteParams)
        throws IOException
    {
        ImmutableList.Builder<Path> baseDirs = ImmutableList.builder();
        ImmutableList.Builder<WorkflowDefinition> defs = ImmutableList.builder();
        for (Path path : dagfilePaths) {
            path = path.normalize().toAbsolutePath();  // this is necessary because Paths.get("abc.yml").getParent() returns null instead of Paths.get("")
            Dagfile dagfile = Dagfile.fromConfig(configLoader.loadParameterizedFile(path.toFile(), overwriteParams));
            // TODO add recursive loading here to support inter-project dependency.
            //      a dependent project will be in a separated namespace (therefore
            //      workflows in the namespace will have package name as prefix like
            //      mylib+wf1 or mylib/sublib+wf2).
            defs.addAll(dagfile.toWorkflowDefinitionList().get());
            baseDirs.add(path.getParent());
        }

        ArchiveMetadata metadata = ArchiveMetadata.of(
                WorkflowDefinitionList.of(defs.build()),
                overwriteParams);

        List<Path> includeFileDirs = baseDirs.build();
        return new ProjectArchive(metadata, (baseDir, consumer) ->
                listFiles(baseDir.toAbsolutePath().normalize(), includeFileDirs, consumer));
    }

    private static void listFiles(Path absBaseDir, Collection<Path> baseDirs, PathConsumer consumer)
        throws IOException
    {
        Set<Path> listed = new HashSet<>();
        for (Path baseDir : baseDirs) {
            Path relPath = absBaseDir.relativize(baseDir.toAbsolutePath().normalize());
            listFilesRecursively(absBaseDir, absBaseDir.resolve(relPath), consumer, listed);
        }
    }

    private static void listFilesRecursively(Path absBaseDir, Path targetDir, PathConsumer consumer, Set<Path> listed)
        throws IOException
    {
        for (Path file : Files.newDirectoryStream(targetDir, archiveFileFilter())) {
            Path relPath = absBaseDir.relativize(file);
            if (listed.add(relPath)) {
                if (Files.isDirectory(file)) {
                    consumer.accept(relPath);
                    listFilesRecursively(absBaseDir, file, consumer, listed);
                }
                else {
                    consumer.accept(relPath);
                }
            }
        }
    }

    private static DirectoryStream.Filter<Path> archiveFileFilter()
    {
        // exclude dot files
        return (target) -> !target.getFileName().toString().startsWith(".");
    }
}
