package io.digdag.cli.client;

import io.digdag.cli.SystemExitException;
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
        for (RestProject proj : projects.getProjects()) {
            ln(" %s", proj.getName());
        }

        ln("");
        err.println("Use `" + programName + " workflows <project-name>` to show details.");
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " projects");
        showCommonOptions();
        return systemExit(error);
    }
}
