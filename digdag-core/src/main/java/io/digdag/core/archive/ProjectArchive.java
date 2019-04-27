package io.digdag.core.archive;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.time.ZoneId;
import java.nio.file.Path;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import static java.util.Locale.ENGLISH;
import static com.google.common.base.Preconditions.checkArgument;

public class ProjectArchive
{
    public static final String WORKFLOW_FILE_SUFFIX = ".dig";

    public static String resourceNameToWorkflowName(String resourceName)
    {
        if (resourceName.endsWith(WORKFLOW_FILE_SUFFIX)) {
            return resourceName.substring(0, resourceName.length() - WORKFLOW_FILE_SUFFIX.length());
        }
        else {
            return resourceName;
        }
    }

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
        listFiles(projectPath, consumer);
    }

    // reused by ProjectArchiveLoader.load
    static void listFiles(Path projectPath, PathConsumer consumer)
        throws IOException
    {
        listFilesRecursively(projectPath, projectPath, consumer, new HashSet<>());
    }

    public String pathToResourceName(Path path)
    {
        return realPathToResourceName(projectPath, path.normalize().toAbsolutePath());
    }

    // reused by WorkflowResourceMatcher.ofSingleFile
    public static String realPathToResourceName(Path projectPath, Path realPath)
    {
        checkArgument(projectPath.isAbsolute(), "project path must be absolute: %s", projectPath);
        checkArgument(realPath.isAbsolute(), "real path must be absolute: %s", realPath);

        if (!realPath.startsWith(projectPath)) {
            throw new IllegalArgumentException(String.format(ENGLISH,
                        "Given path '%s' is outside of project directory '%s'",
                        realPath, projectPath));
        }
        Path relative = projectPath.relativize(realPath);

        // resource name must be separated by '/'. Resource names are used as a part of
        // workflow name later using following resourceNameToWorkflowName method.
        // See also ProjectArchiveLoader.loadWorkflowFile.
        return relative.toString().replace(File.separatorChar, '/');
    }

    private static void listFilesRecursively(Path projectPath, Path targetDir, PathConsumer consumer, Set<String> listed)
        throws IOException
    {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(targetDir, ProjectArchive::rejectDotFiles)) {
            for (Path path : ds) {
                String resourceName = realPathToResourceName(projectPath, path);
                if (listed.add(resourceName)) {
                    consumer.accept(resourceName, path);
                    if (Files.isDirectory(path)) {
                        listFilesRecursively(projectPath, path, consumer, listed);
                    }
                }
            }
        }
    }

    private static boolean rejectDotFiles(Path target)
    {
        return !target.getFileName().toString().startsWith(".");
    }
}
