package io.digdag.cli;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.io.IOException;
import javax.servlet.ServletException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import static io.digdag.cli.Main.systemExit;

public class Sched
    extends Server
{
    private static final Logger logger = LoggerFactory.getLogger(Sched.class);

    @Parameter(names = {"-f", "--file"})
    String dagfilePath = Run.DEFAULT_DAGFILE;

    // TODO no-schedule mode

    @Override
    public void main()
            throws Exception
    {
        if (args.size() != 0) {
            throw usage(null);
        }

        sched();
    }

    @Override
    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag sched [options...]");
        System.err.println("  Options:");
        System.err.println("    -f, --file PATH                  use this file to load tasks (default: digdag.yml)");
        System.err.println("    -n, --port PORT                  port number to listen for web interface and api clients (default: 65432)");
        System.err.println("    -b, --bind ADDRESS               IP address to listen HTTP clients (default: 127.0.0.1)");
        System.err.println("    -o, --database DIR               store status to this database");
        System.err.println("    -c, --config PATH.properties     server configuration property path");
        Main.showCommonOptions();
        return systemExit(error);
    }

    private void sched()
            throws ServletException, IOException
    {
        // use memory database by default
        if (database == null) {
            memoryDatabase = true;
        }

        startServer(dagfilePath);
    }
}
