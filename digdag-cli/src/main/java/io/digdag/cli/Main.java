package io.digdag.cli;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.MissingCommandException;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import io.digdag.cli.client.Archive;
import io.digdag.cli.client.Backfill;
import io.digdag.cli.client.Delete;
import io.digdag.cli.client.Kill;
import io.digdag.cli.client.Push;
import io.digdag.cli.client.Reschedule;
import io.digdag.cli.client.Retry;
import io.digdag.cli.client.Secrets;
import io.digdag.cli.client.ShowAttempt;
import io.digdag.cli.client.ShowAttempts;
import io.digdag.cli.client.ShowLog;
import io.digdag.cli.client.ShowSchedule;
import io.digdag.cli.client.ShowSession;
import io.digdag.cli.client.ShowTask;
import io.digdag.cli.client.ShowWorkflow;
import io.digdag.cli.client.Start;
import io.digdag.cli.client.Upload;
import io.digdag.cli.client.Version;
import io.digdag.core.Environment;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.digdag.cli.ConfigUtil.defaultConfigPath;
import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.core.Version.buildVersion;
import static io.digdag.core.agent.OperatorManager.formatExceptionMessage;

public class Main
{
    private static final String DEFAULT_PROGRAM_NAME = "digdag";

    private final ImmutableCommandContext ctx;

    public Main(io.digdag.core.Version version, Map<String, String> env, PrintStream out, PrintStream err, InputStream in)
    {
        this(ImmutableCommandContext.builder()
                .environment(env)
                .version(version)
                .programName(System.getProperty("io.digdag.cli.programName", DEFAULT_PROGRAM_NAME))
                .in(in)
                .out(out)
                .err(err)
                .build());
    }

    public Main(ImmutableCommandContext ctx)
    {
        this.ctx = Objects.requireNonNull(ctx, "ctx");
    }

    public static class MainOptions
    {
        @Parameter(names = {"-c", "--config"})
        protected String configPath = null;

        @Parameter(names = {"-help", "--help"}, help = true, hidden = true)
        boolean help;
    }

    public static void main(String... args)
    {
        int code = new Main(buildVersion(), System.getenv(), System.out, System.err, System.in).cli(args);
        if (code != 0) {
            System.exit(code);
        }
    }

    public int cli(String... args)
    {
        for (String arg : args) {
            if ("--version".equals(arg)) {
                ctx.out().println(ctx.version().version());
                return 0;
            }
        }
        ctx.err().println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").format(new Date()) + ": Digdag v" + ctx.version());
        if (args.length == 0) {
            usage(null);
            return 0;
        }

        boolean verbose = false;

        MainOptions mainOpts = new MainOptions();
        JCommander jc = new JCommander(mainOpts);
        jc.setProgramName(ctx.programName());

        jc.addCommand("init", new Init(ctx), "new");
        jc.addCommand("run", new Run(ctx), "r");
        jc.addCommand("check", new Check(ctx), "c");
        jc.addCommand("scheduler", new Sched(ctx), "sched");

        jc.addCommand("server", new Server(ctx));

        jc.addCommand("push", new Push(ctx));
        jc.addCommand("archive", new Archive(ctx));
        jc.addCommand("upload", new Upload(ctx));

        jc.addCommand("workflow", new ShowWorkflow(ctx), "workflows");
        jc.addCommand("start", new Start(ctx));
        jc.addCommand("retry", new Retry(ctx));
        jc.addCommand("session", new ShowSession(ctx), "sessions");
        jc.addCommand("attempts", new ShowAttempts(ctx));
        jc.addCommand("attempt", new ShowAttempt(ctx));
        jc.addCommand("reschedule", new Reschedule(ctx));
        jc.addCommand("backfill", new Backfill(ctx));
        jc.addCommand("log", new ShowLog(ctx), "logs");
        jc.addCommand("kill", new Kill(ctx));
        jc.addCommand("task", new ShowTask(ctx), "tasks");
        jc.addCommand("schedule", new ShowSchedule(ctx), "schedules");
        jc.addCommand("delete", new Delete(ctx));
        jc.addCommand("secrets", new Secrets(ctx), "secret");
        jc.addCommand("version", new Version(ctx), "version");

        jc.addCommand("selfupdate", new SelfUpdate(ctx));

        // Disable @ expansion
        jc.setExpandAtSign(false);
        jc.getCommands().values().forEach(c -> c.setExpandAtSign(false));

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
                throw usage(null);
            }

            verbose = processCommonOptions(mainOpts, command);

            command.main();
            return 0;
        }
        catch (ParameterException ex) {
            ctx.err().println("error: " + ex.getMessage());
            return 1;
        }
        catch (SystemExitException ex) {
            if (ex.getMessage() != null) {
                ctx.err().println("error: " + ex.getMessage());
            }
            return ex.getCode();
        }
        catch (Exception ex) {
            String message = formatExceptionMessage(ex);
            if (message.trim().isEmpty()) {
                // prevent silent crash
                ex.printStackTrace(ctx.err());
            }
            else {
                ctx.err().println("error: " + message);
                if (verbose) {
                    ex.printStackTrace(ctx.err());
                }
            }
            return 1;
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

    private boolean processCommonOptions(MainOptions mainOpts, Command command)
            throws SystemExitException
    {
        if (command.help) {
            throw command.usage(null);
        }

        boolean verbose;

        switch (command.logLevel) {
        case "error":
        case "warn":
        case "info":
            verbose = false;
            break;
        case "debug":
        case "trace":
            verbose = true;
            break;
        default:
            throw usage("Unknown log level '"+command.logLevel+"'");
        }

        if (command.configPath == null) {
            command.configPath = mainOpts.configPath;
        }

        configureLogging(command.logLevel, command.logPath);

        for (Map.Entry<String, String> pair : command.systemProperties.entrySet()) {
            System.setProperty(pair.getKey(), pair.getValue());
        }

        return verbose;
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
    private SystemExitException usage(String error)
    {
        ctx.err().println("Usage: " + ctx.programName() + " <command> [options...]");
        ctx.err().println("  Local-mode commands:");
        ctx.err().println("    init <dir>                         create a new workflow project");
        ctx.err().println("    r[un] <workflow.dig>               run a workflow");
        ctx.err().println("    c[heck]                            show workflow definitions");
        ctx.err().println("    sched[uler]                        run a scheduler server");
        ctx.err().println("    selfupdate                         update cli to the latest version");
        ctx.err().println("");
        ctx.err().println("  Server-mode commands:");
        ctx.err().println("    server                             start server");
        ctx.err().println("");
        ctx.err().println("  Client-mode commands:");
        ctx.err().println("    push <project-name>                create and upload a new revision");
        ctx.err().println("    start <project-name> <name>        start a new session attempt of a workflow");
        ctx.err().println("    retry <attempt-id>                 retry a session");
        ctx.err().println("    kill <attempt-id>                  kill a running session attempt");
        ctx.err().println("    backfill <project-name> <name>     start sessions of a schedule for past times");
        ctx.err().println("    reschedule                         skip sessions of a schedule to a future time");
        ctx.err().println("    log <attempt-id>                   show logs of a session attempt");
        ctx.err().println("    workflows [project-name] [name]    show registered workflow definitions");
        ctx.err().println("    schedules                          show registered schedules");
        ctx.err().println("    sessions                           show sessions for all workflows");
        ctx.err().println("    sessions <project-name>            show sessions for all workflows in a project");
        ctx.err().println("    sessions <project-name> <name>     show sessions for a workflow");
        ctx.err().println("    session  <session-id>              show a single session");
        ctx.err().println("    attempts                           show attempts for all sessions");
        ctx.err().println("    attempts <session-id>              show attempts for a session");
        ctx.err().println("    attempt  <attempt-id>              show a single attempt");
        ctx.err().println("    tasks <attempt-id>                 show tasks of a session attempt");
        ctx.err().println("    delete <project-name>              delete a project");
        ctx.err().println("    secrets --project <project-name>   manage secrets");
        ctx.err().println("    version                            show client and server version");
        ctx.err().println("");
        ctx.err().println("  Options:");
        showCommonOptions(ctx);
        if (error == null) {
            ctx.err().println("Use `<command> --help` to see detailed usage of a command.");
            return systemExit(null);
        }
        else {
            return systemExit(error);
        }
    }

    public static void showCommonOptions(CommandContext ctx)
    {
        ctx.err().println("    -L, --log PATH                   output log messages to a file (default: -)");
        ctx.err().println("    -l, --log-level LEVEL            log level (error, warn, info, debug or trace)");
        ctx.err().println("    -X KEY=VALUE                     add a performance system config");
        ctx.err().println("    -c, --config PATH.properties     Configuration file (default: " + defaultConfigPath(ctx.environment()) + ")");
        ctx.err().println("");
    }
}
