package io.digdag.core.archive;

import java.io.IOException;
import java.nio.file.Path;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinitionList;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.core.repository.ModelValidator;
import io.digdag.core.repository.ModelValidationException;
import static io.digdag.core.archive.ProjectArchive.listFiles;
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
            Config overrideParams)
        throws IOException
    {
        // toAbsolutePath is necessary because Paths.get("singleName").getParent() returns null instead of Paths.get("")
        Path projectPath = projectDirectory.normalize().toAbsolutePath();

        // find workflow names
        ModelValidator validator = ModelValidator.builder();
        ImmutableList.Builder<RuntimeException> errors = ImmutableList.builder();
        ImmutableList.Builder<WorkflowDefinition> defs = ImmutableList.builder();

        listFiles(projectPath, (resourceName, path) -> {
            if (matcher.matches(resourceName, path)) {
                try {
                    WorkflowFile workflowFile = loadWorkflowFile(resourceName, path, overrideParams);
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
            return true;
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
                overrideParams);

        return new ProjectArchive(projectPath, metadata);
    }

    private WorkflowFile loadWorkflowFile(String resourceName, Path path,
            Config overrideParams)
        throws IOException
    {
        String workflowName = resourceNameToWorkflowName(resourceName);

        WorkflowFile workflowFile = WorkflowFile.fromConfig(workflowName,
                configLoader.loadParameterizedFile(path.toFile(), overrideParams));

        int posSlash = workflowName.lastIndexOf('/');
        if (posSlash >= 0) {
            // workflow is in a subdirectory. set _workdir accordingly.
            String workdir = workflowName.substring(0, posSlash);
            workflowFile.setBaseWorkdir(workdir);
        }

        return workflowFile;
    }

    public WorkflowFile loadWorkflowFileFromPath(Path projectPath, Path workflowPath,
            Config overrideParams)
        throws IOException
    {
        String resourceName = ProjectArchive.realPathToResourceName(
                projectPath.normalize().toAbsolutePath(),
                workflowPath.normalize().toAbsolutePath());
        return loadWorkflowFile(resourceName, workflowPath, overrideParams);
    }
}
