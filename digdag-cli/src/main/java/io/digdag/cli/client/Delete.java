package io.digdag.cli.client;

import java.io.PrintStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;
import java.io.File;

import com.beust.jcommander.Parameter;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.config.Config;
import io.digdag.core.Version;

import static io.digdag.cli.SystemExitException.systemExit;

public class Delete
    extends ClientCommand
{
    @Parameter(names = {"--force"})
    boolean force = false;

    public Delete(Version version, PrintStream out, PrintStream err)
    {
        super(version, out, err);
    }

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        delete(args.get(0));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag delete <project>");
        err.println("  Options:");
        err.println("        --force                      skip y/N prompt");
        showCommonOptions();
        return systemExit(error);
    }

    private void delete(String projName)
        throws Exception
    {
        DigdagClient client = buildClient();

        RestProject proj = client.getProject(projName);

        ln("Project:");
        ln("  id: %d", proj.getId());
        ln("  name: %s", proj.getName());
        ln("  latest revision: %s", proj.getRevision());
        ln("  created at: %s", TimeUtil.formatTime(proj.getCreatedAt()));
        ln("  last updated at: %s", TimeUtil.formatTime(proj.getUpdatedAt()));

        if (!force) {
            err.print("Are you sure you want to delete this project? [y/N]: ");
            String line;
            try (BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in))) {
                line = stdin.readLine();
            }
            if (!line.trim().equalsIgnoreCase("y") && !line.trim().equalsIgnoreCase("yes")) {
                throw systemExit("canceled.");
            }
        }

        client.deleteProject(proj.getId());
        err.println("Project '" + projName + "' is deleted.");
    }
}
