package io.digdag.core.archive;

import java.nio.file.Path;
import static io.digdag.core.archive.ProjectArchive.realPathToResourceName;
import static io.digdag.core.archive.ProjectArchive.WORKFLOW_FILE_SUFFIX;

public interface WorkflowResourceMatcher
{
    boolean matches(String resourceName);

    public static WorkflowResourceMatcher ofSingleFile(Path projectPath, Path workflowFilePath)
    {
        String exactResourceName = realPathToResourceName(projectPath.normalize().toAbsolutePath(), workflowFilePath.normalize().toAbsolutePath());
        return (resourceName) -> resourceName.equals(exactResourceName);
    }

    public static WorkflowResourceMatcher defaultMatcher()
    {
        // files at current directly ending with .dig
        return (resourceName) -> resourceName.endsWith(WORKFLOW_FILE_SUFFIX) && !resourceName.contains("/");
    }
}
