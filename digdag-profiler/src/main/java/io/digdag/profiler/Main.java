package io.digdag.profiler;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.digdag.client.config.ConfigElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


public class Main
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    static class Args
    {
        @Parameter(names = {"--config", "-c"}, description = "Configuration file path", required = true)
        File configFile;

        @Parameter(names = {"--help", "-h"}, help = true)
        boolean help;
    }

    void run(ConfigElement configElement)
            throws Exception
    {
        new TaskAnalyzer(configElement).run();
    }

    public static void main(final String[] args)
            throws Exception
    {
        Args commandArgs = new Args();
        new JCommander(commandArgs).parse(args);

        Main main = new Main();
        main.run(new ConfigElementLoader(commandArgs.configFile).load());
    }
}
