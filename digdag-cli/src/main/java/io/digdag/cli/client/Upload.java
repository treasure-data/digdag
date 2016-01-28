package io.digdag.cli.client;

import java.util.List;
import java.io.File;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestRepository;
import static io.digdag.cli.Main.systemExit;

public class Upload
    extends ClientCommand
{
    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 3) {
            throw usage(null);
        }
        upload(args.get(0), args.get(1), args.get(2));
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag upload <path.tar.gz> <repository> <revision>");
        System.err.println("  Options:");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void upload(String path, String repoName, String revision)
        throws Exception
    {
        DigdagClient client = buildClient();
        RestRepository repo = client.putRepositoryRevision(repoName, revision, new File(path));
        ln("Uploaded:");
        ln("  id: %d", repo.getId());
        ln("  name: %s", repo.getName());
        ln("  revision: %s", repo.getRevision());
        ln("  archive type: %s", repo.getArchiveType());
        ln("  repository created at: %s", repo.getRevision());
        ln("  revision updated at: %s", repo.getUpdatedAt());
        ln("");
        ln("Use `digdag workflows` to show all workflows.");
    }
}
