package io.digdag.cli;

import java.util.Properties;
import java.util.Map;
import java.util.Date;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Optional;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.MissingCommandException;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.PropertyUtils;
import io.digdag.cli.client.ShowSession;
import io.digdag.cli.client.ShowTask;
import io.digdag.cli.client.ShowWorkflow;
import io.digdag.cli.client.ShowSchedule;
import io.digdag.cli.client.Start;
import io.digdag.cli.client.Reschedule;
import io.digdag.cli.client.Backfill;
import io.digdag.cli.client.Kill;
import io.digdag.cli.client.Upload;

public class Main
{
    private static final String PROGRAM_NAME = "digdag";

    public static class MainOptions
    {
        @Parameter(names = {"-help", "--help"}, help = true, hidden = true)
        boolean help;
    }

    public static void main(String[] args)
        throws Exception
    {
        System.err.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").format(new Date()) + ": Digdag v0.2.0");

        MainOptions mainOpts = new MainOptions();
        JCommander jc = new JCommander(mainOpts);
        jc.setProgramName(PROGRAM_NAME);

        jc.addCommand("new", new Init(), "init");
        jc.addCommand("run", new Run(), "r");
        jc.addCommand("check", new Check(), "c");
        jc.addCommand("scheduler", new Sched(), "sched");

        jc.addCommand("server", new Server());
        jc.addCommand("genapikey", new GenApiKey());

        jc.addCommand("archive", new Archive());
        jc.addCommand("upload", new Upload());

        jc.addCommand("workflow", new ShowWorkflow(), "workflows");
        jc.addCommand("start", new Start());
        jc.addCommand("session", new ShowSession(), "sessions");
        jc.addCommand("reschedule", new Reschedule());
        jc.addCommand("backfill", new Backfill());
        jc.addCommand("kill", new Kill());
        jc.addCommand("task", new ShowTask(), "tasks");
        jc.addCommand("schedule", new ShowSchedule(), "schedules");

        try {
            try {
                jc.parse(args);
            }
            catch (MissingCommandException ex) {
                throw usage("available commands are: "+jc.getCommands().keySet());
            }
            catch (ParameterException ex) {
                if (getParsedCommand(jc) == null) {
                    // go to Run.asImplicit section
                }
                else {
                    throw ex;
                }
            }

            if (mainOpts.help) {
                throw usage(null);
            }

            Command command = getParsedCommand(jc);
            if (command == null) {
                command = Run.asImplicit();
                jc = new JCommander(command);
                jc.setProgramName(PROGRAM_NAME);
                jc.parse(args);
            }

            processCommonOptions(command);

            command.main();
        }
        catch (ParameterException ex) {
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

    private static Command getParsedCommand(JCommander jc)
    {
        String commandName = jc.getParsedCommand();
        if (commandName == null) {
            return null;
        }

        return (Command) jc.getCommands().get(commandName).getObjects().get(0);
    }

    private static void processCommonOptions(Command command)
            throws SystemExitException
    {
        if (command.help) {
            throw command.usage(null);
        }

        switch (command.logLevel) {
        case "error":
        case "warn":
        case "info":
        case "debug":
        case "trace":
            break;
        default:
            throw usage("Unknown log level '"+command.logLevel+"'");
        }

        configureLogging(command.logLevel, command.logPath);

        for (Map.Entry<String, String> pair : command.systemProperties.entrySet()) {
            System.setProperty(pair.getKey(), pair.getValue());
        }
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

    public static Properties loadProperties(String additionalConfigFile)
        throws IOException
    {
        Properties props = new Properties();

        // load from a file
        if (additionalConfigFile != null) {
            props.putAll(PropertyUtils.loadFile(new File(additionalConfigFile)));
        }

        // system property overwrites params
        props.putAll(System.getProperties());

        return props;
    }

    // called also by Run
    static SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag <command> [options...]");
        System.err.println("  Local-mode commands:");
        System.err.println("    new <path>                       create a new workflow project");
        System.err.println("    r[un] [+name]                    run a workflow");
        System.err.println("    c[heck]                          show workflow definitions");
        System.err.println("    sched[uler]                      run a scheduler server");
        System.err.println("");
        System.err.println("  Server-mode commands:");
        System.err.println("    server                           start digdag server");
        System.err.println("    genapikey -o <dir>               generates an API key for authentication");
        System.err.println("");
        System.err.println("  Client-mode commands:");
        System.err.println("    start <repo-name> <+name>        start a new session of a workflow");
        System.err.println("    kill <session-id>                kill a running session");
        System.err.println("    workflows [+name]                show registered workflow definitions");
        System.err.println("    schedules                        show registered schedules");
        System.err.println("    sessions [repo-name] [+name]     show past and current sessions");
        System.err.println("    tasks <session-id>               show tasks of a session");
        System.err.println("    archive <workflow.yml...>        create a project archive");
        System.err.println("    upload <workflow.yml...>         upload a project archive to a server");
        System.err.println("");
        System.err.println("  Options:");
        showCommonOptions();
        if (error == null) {
            System.err.println("Use `<command> --help` to see detailed usage of a command.");
            return systemExit(null);
        }
        else {
            return systemExit(error);
        }
    }

    public static void showCommonOptions()
    {
        System.err.println("    -g, --log PATH                   output log messages to a file (default: -)");
        System.err.println("    -l, --log-level LEVEL            log level (error, warn, info, debug or trace)");
        System.err.println("    -X KEY=VALUE                     add a performance system config");
        System.err.println("");
    }

    public static SystemExitException systemExit(String errorMessage)
    {
        if (errorMessage != null) {
            return new SystemExitException(1, errorMessage);
        }
        else {
            return new SystemExitException(0, null);
        }
    }
}
