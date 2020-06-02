package io.digdag.cli.client;

import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestRevision;

import javax.ws.rs.NotFoundException;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowRevisions extends ClientCommande {
  @Override
  public void mainWithClientException()
          throws Exception
  {
      switch (args.size()) {
          case 1:
            showRevisions(args.get(0));
          break;
          default:
              throw usage(null);
      }
  }

    private void showRevisions(String projectid) throws Exception
    {
        DigdagClient client = buildClient();

        RestRevisionCollection projects = client.getRevisions(projectid);
        ln("Revisions");
        for (RestRevision revision : projects.geRevisions()) {
            showRevisionDetail(revision);
        }
        err.println("Use `" + programName + " revisions <projectid> to show details.");
    }

    private void showRevisionDetail(RestRevision revision)
    {
        ln("  id: %s", revision.getRevision());
        ln("  archive type: %s", project.getArchiveType());
        ln("  revision updated at: %s", TimeUtil.formatTime(project.getUpdatedAt()));
        String yaml = yamlMapper().toYaml(revision.getUserInfo());
        ln("%s", yaml);
        ln("");
    }

    public SystemExitException usage(String error) {
        err.println("Usage: " + programName + " revisions [projectid] [revisionid]");
        showCommonOptions();
        return systemExit(error);
    }
}
