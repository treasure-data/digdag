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
import io.digdag.client.config.ConfigException;
import io.digdag.core.repository.ModelValidator;
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

    public ProjectArchive loadProject(
            Path dagfilePath,
            Config overwriteParams)
        throws IOException
    {
        return load(dagfilePath, overwriteParams, false);
    }

    public ProjectArchive loadProjectOrSingleWorkflow(
            Path dagfilePath,
            Config overwriteParams)
        throws IOException
    {
        return load(dagfilePath, overwriteParams, true);
    }

    private ProjectArchive load(
            Path dagfilePath,
            Config overwriteParams,
            boolean singleWorkflowAllowed)
        throws IOException
    {
        Path path = dagfilePath.normalize().toAbsolutePath();  // this is necessary because Paths.get("abc.yml").getParent() returns null instead of Paths.get("")

        Config projectConfig = configLoader.loadParameterizedFile(path.toFile(), overwriteParams);

        if (singleWorkflowAllowed && !projectConfig.has("workflows")) {
            return loadSingleWorkflowProject(path, overwriteParams);
        }

        Path projectDir = path.getParent();
        ProjectFile projectFile = ProjectFile.fromConfig(projectConfig);

        Config projectParams = projectFile.getDefaultParams().deepCopy().setAll(overwriteParams);

        ImmutableList.Builder<WorkflowDefinition> defs = ImmutableList.builder();
        for (String relativeFileName : projectFile.getWorkflowFiles()) {
            Path workflowFilePath = projectDir.resolve(relativeFileName).normalize();

            try {
                WorkflowFile workflowFile = loadWorkflowFile(projectDir, workflowFilePath, projectFile.getDefaultTimeZone(), projectParams);

                defs.add(workflowFile.toWorkflowDefinition());
            }
            catch (Exception ex) {
                throw new ConfigException("Failed to load a workflow file: " + workflowFilePath, ex);
            }
        }

        ArchiveMetadata metadata = ArchiveMetadata.of(
                WorkflowDefinitionList.of(defs.build()),
                projectParams);

        return new ProjectArchive(metadata, (baseDir, consumer) ->
                listFiles(baseDir, projectDir, consumer));
    }

    private ProjectArchive loadSingleWorkflowProject(Path path, Config overwriteParams)
        throws IOException
    {
        Path dir = path.getParent();
        WorkflowFile workflowFile = loadWorkflowFile(dir, path, ZoneId.of("UTC"), overwriteParams);

        ArchiveMetadata metadata = ArchiveMetadata.of(
                WorkflowDefinitionList.of(ImmutableList.of(workflowFile.toWorkflowDefinition())),
                overwriteParams);

        return new ProjectArchive(metadata, (baseDir, consumer) ->
                listFiles(baseDir, dir, consumer));
    }

    private WorkflowFile loadWorkflowFile(Path projectDir, Path workflowFilePath,
            ZoneId defaultTimeZone, Config projectParams)
        throws IOException
    {
        // remove last .yml and use it as workflowName
        String fileName = workflowFilePath.getFileName().toString();
        int lastPosDot = fileName.lastIndexOf('.');
        String workflowName;
        if (lastPosDot > 0) {
            workflowName = fileName.substring(0, lastPosDot);
        }
        else {
            workflowName = fileName;
        }

        WorkflowFile workflowFile = WorkflowFile.fromConfig(
                workflowName, defaultTimeZone,
                configLoader.loadParameterizedFile(workflowFilePath.toFile(), projectParams));

        Path workflowSubdir = projectDir.relativize(workflowFilePath).getParent();
        if (workflowSubdir != null) {
            // workflow file is in a subdirectory. set _workdir to point the directory
            workflowFile.setWorkdir(
                    projectParams.getOptional("_workdir", String.class)
                    .transform(it -> it + "/" + workflowSubdir)
                    .or(workflowSubdir.toString()));
        }

        return workflowFile;
    }

    private static void listFiles(Path baseDir, Path dir, PathConsumer consumer)
        throws IOException
    {
        Path absBaseDir = baseDir.toAbsolutePath().normalize();
        Path relPath = absBaseDir.relativize(dir.toAbsolutePath().normalize());
        listFilesRecursively(absBaseDir, absBaseDir.resolve(relPath), consumer, new HashSet<>());
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
