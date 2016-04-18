package io.digdag.core.archive;

import java.io.IOException;
import java.time.ZoneId;
import java.nio.file.Path;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;
import io.digdag.core.repository.WorkflowDefinitionList;

public class ProjectArchive
{
    public interface PathListing
    {
        public void list(Path baseDir, PathConsumer consumer) throws IOException;
    }

    public interface PathConsumer
    {
        public void accept(Path path) throws IOException;
    }

    private final ArchiveMetadata metadata;
    private final PathListing pathListing;

    ProjectArchive(ArchiveMetadata metadata, PathListing pathListing)
    {
        this.metadata = metadata;
        this.pathListing = pathListing;
    }

    public ArchiveMetadata getMetadata()
    {
        return metadata;
    }

    public void listFiles(Path baseDir, PathConsumer consumer)
        throws IOException
    {
        pathListing.list(baseDir, consumer);
    }
}
