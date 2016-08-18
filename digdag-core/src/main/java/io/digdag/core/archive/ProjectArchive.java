package io.digdag.core.archive;

import java.io.IOException;
import java.time.ZoneId;
import java.nio.file.Path;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;
import io.digdag.core.repository.WorkflowDefinitionList;
import static java.util.Locale.ENGLISH;
import static com.google.common.base.Preconditions.checkArgument;

public class ProjectArchive
{
    public static final String WORKFLOW_FILE_SUFFIX = ".dig";

    public interface PathConsumer
    {
        public void accept(String resourceName, Path absolutePath) throws IOException;
    }

    private final Path projectPath;
    private final ArchiveMetadata metadata;

    ProjectArchive(Path projectPath, ArchiveMetadata metadata)
    {
        checkArgument(projectPath.isAbsolute(), "project path must be absolute: %s", projectPath);
        this.projectPath = projectPath;
        this.metadata = metadata;
    }

    public Path getProjectPath()
    {
        return projectPath;
    }

    public ArchiveMetadata getArchiveMetadata()
    {
        return metadata;
    }

    public void listFiles(PathConsumer consumer)
        throws IOException
    {
        ProjectArchiveLoader.listFiles(projectPath, consumer);
    }

    public String pathToResourceName(Path path)
    {
        return realPathToResourceName(projectPath, path.normalize().toAbsolutePath());
    }

    static String realPathToResourceName(Path projectPath, Path realPath)
    {
        checkArgument(projectPath.isAbsolute(), "project path must be absolute: %s", projectPath);
        checkArgument(realPath.isAbsolute(), "real path must be absolute: %s", realPath);

        if (!realPath.startsWith(projectPath)) {
            throw new IllegalArgumentException(String.format(ENGLISH,
                        "Given path '%s' is outside of project directory '%s'",
                        realPath, projectPath));
        }
        Path relative = projectPath.relativize(realPath);
        return relative.toString();  // TODO make sure path names are separated by '/'
    }

    public static String resourceNameToWorkflowName(String resourceName)
    {
        if (resourceName.endsWith(WORKFLOW_FILE_SUFFIX)) {
            return resourceName.substring(0, resourceName.length() - WORKFLOW_FILE_SUFFIX.length());
        }
        else {
            return resourceName;
        }
    }
}
