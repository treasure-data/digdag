package io.digdag.cli.client;

import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestProjectCollection;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowProjects
        extends ClientCommand
{
    @Override
    public void mainWithClientException()
            throws Exception
    {
        DigdagClient client = buildClient();

        RestProjectCollection projects = client.getProjects();
        ln("Projects");
        for (RestProject proj : projects.getProjects()) {
            ln("  name: %s", proj.getName());
            ln("  id: %s", proj.getId());
            ln("  revision: %s", proj.getRevision());
            ln("  archive type: %s", proj.getArchiveType());
            ln("  project created at: %s", TimeUtil.formatTime(proj.getCreatedAt()));
            ln("  revision updated at: %s", TimeUtil.formatTime(proj.getUpdatedAt()));
            ln("");
        }
        err.println("Use `" + programName + " workflows <project-name>` to show details.");
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " projects");
        showCommonOptions();
        return systemExit(error);
    }
}
