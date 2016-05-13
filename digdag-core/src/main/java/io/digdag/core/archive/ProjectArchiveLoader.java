package io.digdag.core.archive;

import java.io.IOException;
import java.util.Properties;
import java.util.HashSet;
import java.util.Set;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinitionList;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.repository.ModelValidator;
import io.digdag.core.repository.ModelValidationException;
import io.digdag.core.archive.ProjectArchive.PathConsumer;
import static io.digdag.core.archive.ProjectArchive.realPathToResourceName;
import static io.digdag.core.archive.ProjectArchive.resourceNameToWorkflowName;

public class ProjectArchiveLoader
{
    private final ConfigLoaderManager configLoader;

    @Inject
    public ProjectArchiveLoader(ConfigLoaderManager configLoader)
    {
        this.configLoader = configLoader;
    }

    public ProjectArchive load(
            Path projectDirectory,
            WorkflowResourceMatcher matcher,
            Config overwriteParams)
        throws IOException
    {
        // toAbsolutePath is necessary because Paths.get("singleName").getParent() returns null instead of Paths.get("")
        Path projectPath = projectDirectory.normalize().toAbsolutePath();

        // find workflow names
        ModelValidator validator = ModelValidator.builder();
        ImmutableList.Builder<RuntimeException> errors = ImmutableList.builder();
        ImmutableList.Builder<WorkflowDefinition> defs = ImmutableList.builder();

        listFiles(projectPath, (resourceName, path) -> {
            if (matcher.matches(resourceName)) {
                try {
                    WorkflowFile workflowFile = loadWorkflowFile(resourceName, path, overwriteParams);
                    defs.add(workflowFile.toWorkflowDefinition());
                }
                catch (IOException ex) {
                    throw ex;
                }
                catch (RuntimeException ex) {
                    validator.error("workflow " + path, null, ex.getMessage());
                    errors.add(ex);
                }
            }
        });

        try {
            validator.validate("project", projectDirectory);
        }
        catch (ModelValidationException ex) {
            for (Throwable error : errors.build()) {
                ex.addSuppressed(error);
            }
            throw ex;
        }

        ArchiveMetadata metadata = ArchiveMetadata.of(
                WorkflowDefinitionList.of(defs.build()),
                overwriteParams);

        return new ProjectArchive(projectPath, metadata);
    }

    private WorkflowFile loadWorkflowFile(String resourceName, Path path,
            Config overwriteParams)
        throws IOException
    {
        String workflowName = resourceNameToWorkflowName(resourceName);

        WorkflowFile workflowFile = WorkflowFile.fromConfig(workflowName,
                configLoader.loadParameterizedFile(path.toFile(), overwriteParams));

        int posSlash = workflowName.lastIndexOf('/');
        if (posSlash >= 0) {
            // workflow is in a subdirectory. set _workdir accordingly.
            String workdir = overwriteParams.getOptional("_workdir", String.class)  // overwriteParams has higher priority
                .or(workflowName.substring(0, posSlash));
            workflowFile.setWorkdir(workdir);
        }

        return workflowFile;
    }

    static void listFiles(Path projectPath, PathConsumer consumer)
        throws IOException
    {
        listFilesRecursively(projectPath, projectPath, consumer, new HashSet<>());
    }

    private static void listFilesRecursively(Path projectPath, Path targetDir, PathConsumer consumer, Set<String> listed)
        throws IOException
    {
        for (Path path : Files.newDirectoryStream(targetDir, ProjectArchiveLoader::rejectDotFiles)) {
            String resourceName = realPathToResourceName(projectPath, path);
            if (listed.add(resourceName)) {
                consumer.accept(resourceName, path);
                if (Files.isDirectory(path)) {
                    listFilesRecursively(projectPath, path, consumer, listed);
                }
            }
        }
    }

    private static boolean rejectDotFiles(Path target)
    {
        return !target.getFileName().toString().startsWith(".");
    }
}
