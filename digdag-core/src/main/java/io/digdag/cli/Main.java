package io.digdag.cli;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import static java.util.Arrays.asList;

public class Main
{
    public static void main(String[] args)
            throws Exception
    {
        try {
            run(args);
        }
        catch (SystemExitException e) {
            System.exit(e.getCode());
        }
    }

    public static void run(String[] args)
            throws Exception
    {
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
        default:
            throw usage("Unknown command '"+command+"'");
        }
    }

    static OptionParser parser()
    {
        OptionParser parser = new OptionParser();
        parser.accepts("help");
        parser.acceptsAll(asList("L", "log-level")).withRequiredArg().ofType(String.class).defaultsTo("info");
        parser.acceptsAll(asList("l", "log")).withRequiredArg().ofType(String.class).defaultsTo("-");
        parser.acceptsAll(asList("X")).withRequiredArg().ofType(String.class);
        return parser;
    }

    static OptionSet parse(OptionParser parser, String[] args)
            throws SystemExitException
    {
        OptionSet op = parser.parse(args);

        String logLevel = (String) op.valueOf("L");
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

        configureLogging(logLevel, (String) op.valueOf("l"));

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
        System.err.println("    run <workflow.yml>               Run log messages to a file (default: -)");
        System.err.println("");
        System.err.println("  Options:");
        System.err.println("    -l, --log PATH                   Output log messages to a file (default: -)");
        System.err.println("    -L, --log-level LEVEL            Log level (error, warn, info, debug or trace)");
        System.err.println("    -X KEY=VALUE                     Add a performance system config");
        System.err.println("");
        if (error == null) {
            System.err.println("Use `<command> --help` to see detailed usage of a command.");
            return systemExit(null);
        }
        else {
            return systemExit(error);
        }
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
            System.err.println("error: "+errorMessage);
            return new SystemExitException(1, errorMessage);
        }
        else {
            return new SystemExitException(0, null);
        }
    }
}
