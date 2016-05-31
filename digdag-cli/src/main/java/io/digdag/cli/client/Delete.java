package io.digdag.cli.client;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.HashMap;
import java.io.File;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.digdag.cli.Run;
import io.digdag.cli.StdErr;
import io.digdag.cli.StdOut;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.YamlMapper;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.Version;
import io.digdag.core.config.ConfigLoaderManager;

import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.SystemExitException.systemExit;

public class Delete
    extends ClientCommand
{
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
        showCommonOptions();
        return systemExit(error);
    }

    private void delete(String projName)
        throws Exception
    {
        DigdagClient client = buildClient();

        RestProject proj = client.getProject(projName);

        client.deleteProject(proj.getId());
        err.println("Project '" + projName + "' is deleted.");
    }
}
