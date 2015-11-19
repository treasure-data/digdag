package io.digdag.cli;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionException;
import io.digdag.DigdagEmbed;
import static java.util.Arrays.asList;

public class Main
{
    public static void main(String[] args)
            throws Exception
    {
        try {
            run(args);
        }
        catch (OptionException ex) {
            System.err.println("error: "+ex.getMessage());
            System.exit(1);
        }
        catch (SystemExitException ex) {
            if (ex.getMessage() != null) {
                System.err.println("error: "+ex.getMessage());
            }
            System.exit(ex.getCode());
        }
    }

    public static void run(String[] args)
            throws Exception
    {
        System.err.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").format(new Date())+ ": Digdag v0.1.0");

        List<String> argList = new ArrayList<>(asList(args));
        String command = argList.stream().filter(a -> !a.startsWith("-")).findFirst().orElse(null);
        if (command == null) {
            throw usage(null);
        }

        argList.remove(command);
        String[] commandArgs = argList.toArray(new String[argList.size()]);

        switch (command) {
        case "run":
            Run.main(command, commandArgs);
            break;
        case "sched":
            Sched.main(command, commandArgs);
            break;
        case "show":
            Show.main(command, commandArgs);
            break;
        case "archive":
            Archive.main(command, commandArgs);
            break;
        case "server":
            Server.main(command, commandArgs);
            break;
        default:
            throw usage("Unknown command '"+command+"'");
        }
    }

    static OptionParser parser()
    {
        OptionParser parser = new OptionParser();
        parser.accepts("help");
        parser.acceptsAll(asList("l", "log-level")).withRequiredArg().ofType(String.class).defaultsTo("info");
        parser.acceptsAll(asList("g", "log")).withRequiredArg().ofType(String.class).defaultsTo("-");
        parser.acceptsAll(asList("X")).withRequiredArg().ofType(String.class);
        return parser;
    }

    static OptionSet parse(OptionParser parser, String[] args)
            throws SystemExitException
    {
        OptionSet op = parser.parse(args);

        String logLevel = (String) op.valueOf("l");
        switch (logLevel) {
        case "error":
        case "warn":
        case "info":
        case "debug":
        case "trace":
            logLevel = logLevel.toUpperCase();
            break;
        default:
            throw usage("Unknown log level '"+logLevel+"'");
        }

        configureLogging(logLevel, (String) op.valueOf("g"));

        for (Object kv : op.valuesOf("X")) {
            String[] pair = kv.toString().split("=", 2);
            String key = pair[0];
            String value = (pair.length > 1 ? pair[1] : "true");
            System.setProperty(key, value);
        }

        return op;
    }

    private static void configureLogging(String level, String logPath)
    {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        context.reset();

        String name;
        if (logPath.equals("-")) {
            if (System.console() != null) {
                name = "/digdag/cli/logback-color.xml";
            } else {
                name = "/digdag/cli/logback-console.xml";
            }
        } else {
            // logback uses system property to embed variables in XML file
            System.setProperty("digdag.logPath", logPath);
            name = "/digdag/cli/logback-file.xml";
        }
        try {
            configurator.doConfigure(Main.class.getResource(name));
        } catch (JoranException ex) {
            throw new RuntimeException(ex);
        }

        Logger logger = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        if (logger instanceof ch.qos.logback.classic.Logger) {
            ((ch.qos.logback.classic.Logger) logger).setLevel(Level.toLevel(level.toUpperCase(), Level.DEBUG));
        }
    }

    static List<String> nonOptions(OptionSet op)
    {
        List<?> following = op.nonOptionArguments();
        return Arrays.asList(following.toArray(new String[following.size()]));
    }

    private static SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag <command> [options...]");
        System.err.println("  Commands:");
        System.err.println("    run <workflow.yml>               run a workflow");
        System.err.println("    show <workflow.yml>              visualize a workflow");
        System.err.println("    sched <workflow.yml> -o <dir>    start scheduling a workflow");
        System.err.println("");
        System.err.println("  Server-mode commands:");
        System.err.println("    archive <workflow.yml...>        create a project archive");
        System.err.println("    server                           start digdag server");
        System.err.println("");
        System.err.println("  Options:");
        showCommonOptions();
        System.err.println("");
        if (error == null) {
            System.err.println("Use `<command> --help` to see detailed usage of a command.");
            return systemExit(null);
        }
        else {
            return systemExit(error);
        }
    }

    static void showCommonOptions()
    {
        System.err.println("    -g, --log PATH                   output log messages to a file (default: -)");
        System.err.println("    -l, --log-level LEVEL            log level (error, warn, info, debug or trace)");
        System.err.println("    -X KEY=VALUE                     add a performance system config");
    }

    public static class SystemExitException
            extends Exception
    {
        private final int code;

        public SystemExitException(int code, String message)
        {
            super(message);
            this.code = code;
        }

        public int getCode()
        {
            return code;
        }
    }

    static SystemExitException systemExit(String errorMessage)
    {
        if (errorMessage != null) {
            return new SystemExitException(1, errorMessage);
        }
        else {
            return new SystemExitException(0, null);
        }
    }

    static DigdagEmbed embed()
    {
        return new DigdagEmbed.Bootstrap().initialize();
    }
}
