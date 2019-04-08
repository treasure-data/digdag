package io.digdag.cli.client;

import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestProjectCollection;

import javax.ws.rs.NotFoundException;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowProjects
        extends ClientCommand
{
    @Override
    public void mainWithClientException()
            throws Exception
    {
        switch (args.size()) {
            case 0:
                showProjects();
                break;
            case 1:
                showSingleProject(args.get(0));
                break;
            default:
                throw usage(null);
        }
    }

    private void showProjects()
            throws Exception
    {
        DigdagClient client = buildClient();

        RestProjectCollection projects = client.getProjects();
        ln("Projects");
        for (RestProject project : projects.getProjects()) {
            showProjectDetail(project);
        }
        err.println("Use `" + programName + " workflows <project-name>` to show details.");
    }

    private void showSingleProject(String name)
            throws Exception
    {
        DigdagClient client = buildClient();
        ln("Projects");
        try {
            RestProject project = client.getProject(name);
            showProjectDetail(project);
            err.println("Use `" + programName + " workflows <project-name>` to show details.");
        }
        catch (NotFoundException ex) {
            ln("  Not found");
        }
    }

    private void showProjectDetail(RestProject project)
    {
        ln("  name: %s", project.getName());
        ln("  id: %s", project.getId());
        ln("  revision: %s", project.getRevision());
        ln("  archive type: %s", project.getArchiveType());
        ln("  project created at: %s", TimeUtil.formatTime(project.getCreatedAt()));
        ln("  revision updated at: %s", TimeUtil.formatTime(project.getUpdatedAt()));
        ln("");
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " projects [project]");
        showCommonOptions();
        return systemExit(error);
    }
}
