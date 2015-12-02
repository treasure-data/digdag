package io.digdag.standards;

import java.io.File;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.core.spi.CommandExecutor;
import io.digdag.core.spi.RevisionInfo;

public class SimpleCommandExecutor
    implements CommandExecutor
{
    // TODO make these parameters configurable
    private final boolean extractArchive = false;
    private final File archiveBuildPath = new File("tmp/");

    @Inject
    public SimpleCommandExecutor()
    {
    }

    public Process start(
            Optional<RevisionInfo> archiveRevision,
            ProcessBuilder pb)
        throws IOException
    {
        if (archiveRevision.isPresent() && extractArchive) {
            // TODO get File of revisionInfo from the injected ArchiveManager and
            //      extract it to a new temp directory (archiveBuildPath/${repositoryName}/${revisionName}.${revisionId}.tmp)
            //      use Files.move to move _tmp to the actual dir with CopyOption.ATOMIC_MOVE
        }
        return pb.start();
    }
}
