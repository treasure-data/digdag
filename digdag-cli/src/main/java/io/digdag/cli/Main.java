package io.digdag.cli;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.digdag.cli.client.Archive;
import io.digdag.cli.client.Backfill;
import io.digdag.cli.client.Kill;
import io.digdag.cli.client.Push;
import io.digdag.cli.client.Reschedule;
import io.digdag.cli.client.Retry;
import io.digdag.cli.client.ShowAttempt;
import io.digdag.cli.client.ShowLog;
import io.digdag.cli.client.ShowSchedule;
import io.digdag.cli.client.ShowSession;
import io.digdag.cli.client.ShowTask;
import io.digdag.cli.client.ShowWorkflow;
import io.digdag.cli.client.Start;
import io.digdag.cli.client.Upload;
import io.digdag.cli.client.Version;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.digdag.cli.SystemExitException.systemExit;

import static io.digdag.cli.ConfigUtil.defaultConfigPath;
import static io.digdag.core.Version.buildVersion;

public class Main
{
    private static final String PROGRAM_NAME = "digdag";

    private final io.digdag.core.Version version;
    private final PrintStream out;
    private final PrintStream err;

    public Main(io.digdag.core.Version version, PrintStream out, PrintStream err)
    {
        this.version = version;
        this.out = out;
        this.err = err;
    }

    public static class MainOptions
    {
        @Parameter(names = {"-help", "--help"}, help = true, hidden = true)
        boolean help;
    }

    public static void main(String... args)
    {
        int code = new Main(buildVersion(), System.out, System.err).cli(args);
        if (code != 0) {
            System.exit(code);
        }
    }

    public int cli(String... args)
    {
        if (args.length == 1 && args[0].equals("--version")) {
            out.println(version.version());
            return 0;
        }
        err.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").format(new Date()) + ": Digdag v" + version);

        MainOptions mainOpts = new MainOptions();
        JCommander jc = new JCommander(mainOpts);
        jc.setProgramName(PROGRAM_NAME);

        jc.addCommand("init", new Init(out, err), "new");
        jc.addCommand("run", new Run(out, err), "r");
        jc.addCommand("check", new Check(out, err), "c");
        jc.addCommand("scheduler", new Sched(version, out, err), "sched");

        jc.addCommand("server", new Server(version, out, err));

        jc.addCommand("push", new Push(version, out, err));
        jc.addCommand("archive", new Archive(out, err));
        jc.addCommand("upload", new Upload(version, out, err));

        jc.addCommand("workflow", new ShowWorkflow(version, out, err), "workflows");
        jc.addCommand("start", new Start(version, out, err));
        jc.addCommand("retry", new Retry(version, out, err));
        jc.addCommand("session", new ShowSession(version, out, err), "sessions");
        jc.addCommand("atteempt", new ShowAttempt(version, out, err), "attempts");
        jc.addCommand("reschedule", new Reschedule(version, out, err));
        jc.addCommand("backfill", new Backfill(version, out, err));
        jc.addCommand("log", new ShowLog(version, out, err), "logs");
        jc.addCommand("kill", new Kill(version, out, err));
        jc.addCommand("task", new ShowTask(version, out, err), "tasks");
        jc.addCommand("schedule", new ShowSchedule(version, out, err), "schedules");
        jc.addCommand("version", new Version(version, out, err), "version");

        jc.addCommand("selfupdate", new SelfUpdate(out, err));

        try {
            try {
                jc.parse(args);
            }
            catch (MissingCommandException ex) {
                throw usage(err, "available commands are: "+jc.getCommands().keySet());
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
                throw usage(err, null);
            }

            Command command = getParsedCommand(jc);
            if (command == null) {
                throw usage(err, null);
            }

            processCommonOptions(err, command);

            command.main();
            return 0;
        }
        catch (ParameterException ex) {
            err.println("error: " + ex.getMessage());
            return 1;
        }
        catch (SystemExitException ex) {
            if (ex.getMessage() != null) {
                err.println("error: " + ex.getMessage());
            }
            return ex.getCode();
        }
        catch (Exception ex) {
            err.println("error: " + formatException(ex));
            return 1;
        }
    }

    private static String formatException(Exception ex)
    {
        StringBuilder sb = new StringBuilder();
        collectExceptionMessage(sb, ex, new HashSet<>());
        return sb.toString();
    }

    private static void collectExceptionMessage(StringBuilder sb, Throwable ex, Set<String> used)
    {
        if (ex.getMessage() != null && used.add(ex.getMessage())) {
            if (sb.length() > 0) {
                sb.append("\n> ");
            }
            sb.append(ex.getMessage());
        }
        if (ex.getCause() != null) {
            collectExceptionMessage(sb, ex.getCause(), used);
        }
        for (Throwable t : ex.getSuppressed()) {
            collectExceptionMessage(sb, t, used);
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

    private static void processCommonOptions(PrintStream err, Command command)
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
            throw usage(err, "Unknown log level '"+command.logLevel+"'");
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

        // logback uses system property to embed variables in XML file
        Level lv = Level.toLevel(level.toUpperCase(), Level.DEBUG);
        System.setProperty("digdag.log.level", lv.toString());

        String name;
        if (logPath.equals("-")) {
            if (System.console() != null) {
                name = "/digdag/cli/logback-color.xml";
            } else {
                name = "/digdag/cli/logback-console.xml";
            }
        } else {
            System.setProperty("digdag.log.path", logPath);
            name = "/digdag/cli/logback-file.xml";
        }
        try {
            configurator.doConfigure(Main.class.getResource(name));
        } catch (JoranException ex) {
            throw new RuntimeException(ex);
        }
    }

    // called also by Run
    static SystemExitException usage(PrintStream err, String error)
    {
        err.println("Usage: digdag <command> [options...]");
        err.println("  Local-mode commands:");
        err.println("    new <path>                       create a new workflow project");
        err.println("    r[un] [name]                     run a workflow");
        err.println("    c[heck]                          show workflow definitions");
        err.println("    sched[uler]                      run a scheduler server");
        err.println("    selfupdate                       update digdag to the latest version");
        err.println("");
        err.println("  Server-mode commands:");
        err.println("    server                           start digdag server");
        err.println("");
        err.println("  Client-mode commands:");
        err.println("    push <project-name>              create and upload a new revision");
        err.println("    start <project-name> <name>      start a new session attempt of a workflow");
        err.println("    retry <attempt-id>               retry a session");
        err.println("    kill <attempt-id>                kill a running session attempt");
        err.println("    backfill                         start sessions of a schedule for past times");
        err.println("    reschedule                       skip sessions of a schedule to a future time");
        err.println("    log <attempt-id>                 show logs of a session attempt");
        err.println("    workflows [project-name] [name]  show registered workflow definitions");
        err.println("    schedules                        show registered schedules");
        err.println("    sessions [project-name] [name]   show past and current sessions");
        err.println("    sessions [attempt-id]            show a single sessions");
        err.println("    attempts [project-name] [name]   show past and current session attempts");
        err.println("    attempts [attempt-id]            show a single session attempt");
        err.println("    tasks <attempt-id>               show tasks of a session attempt");
        err.println("    version                          show client and server version");
        err.println("");
        err.println("  Options:");
        showCommonOptions(err);
        if (error == null) {
            err.println("Use `<command> --help` to see detailed usage of a command.");
            return systemExit(null);
        }
        else {
            return systemExit(error);
        }
    }

    public static void showCommonOptions(PrintStream err)
    {
        err.println("    -L, --log PATH                   output log messages to a file (default: -)");
        err.println("    -l, --log-level LEVEL            log level (error, warn, info, debug or trace)");
        err.println("    -X KEY=VALUE                     add a performance system config");
        err.println("    -c, --config PATH.properties     Configuration file (default: " + defaultConfigPath() + ")");
        err.println("");
    }
}
