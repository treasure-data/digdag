package io.digdag.core.archive;

import java.nio.file.Path;
import java.nio.file.Files;
import static io.digdag.core.archive.ProjectArchive.realPathToResourceName;
import static io.digdag.core.archive.ProjectArchive.WORKFLOW_FILE_SUFFIX;

public interface WorkflowResourceMatcher
{
    boolean matches(String resourceName, Path absolutePath);

    public static WorkflowResourceMatcher ofSingleFile(Path projectPath, Path workflowFilePath)
    {
        String exactResourceName = realPathToResourceName(projectPath.normalize().toAbsolutePath(), workflowFilePath.normalize().toAbsolutePath());
        return (resourceName, absolutePath) -> resourceName.equals(exactResourceName);
    }

    public static WorkflowResourceMatcher defaultMatcher()
    {
        // files at the top directory ending with .dig
        return (resourceName, absolutePath) ->
            resourceName.endsWith(WORKFLOW_FILE_SUFFIX)
            && !resourceName.contains("/")
            && Files.isRegularFile(absolutePath);
    }
}
