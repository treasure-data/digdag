package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.core.archive.ProjectArchives.ExtractListener;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import static io.digdag.core.archive.ProjectArchives.extractTarArchive;
import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.cli.client.ProjectUtil.showUploadedProject;

public class Download
    extends ClientCommand
{
    @Parameter(names = {"-r", "--revision"})
    String revision = null;

    @Parameter(names = {"-o", "--output"})
    String output = null;

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        download(args.get(0));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " download <project>");
        err.println("  Options:");
        err.println("    -r, --revision REVISION          specific revision name instead of the latest revision");
        err.println("    -o, --output DIR                 extract files in this output directory (default: same with project name)");
        showCommonOptions();
        return systemExit(error);
    }

    private void download(String projName)
        throws IOException, SystemExitException
    {
        DigdagClient client = buildClient();

        RestProject project = client.getProject(projName);

        if (revision == null) {
            revision = project.getRevision();
        }

        if (output == null) {
            output = projName;
        }

        final Path destDir = Paths.get(output);
        Files.createDirectories(destDir);

        extractTarArchive(destDir, client.getProjectArchive(project.getId(), revision), new ExtractListener() {
            @Override
            public void file(Path file)
            {
                ln("  %s", destDir.resolve(file));
            }

            @Override
            public void symlink(Path file, String dest)
            {
                ln("  %s -> %s", destDir.resolve(file), dest);
            }
        });

        ln("Extracted project '%s' revision '%s' to %s.", projName, revision, output);
    }
}
