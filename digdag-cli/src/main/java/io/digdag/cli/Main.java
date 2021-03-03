package io.digdag.cli;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
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
import io.digdag.cli.client.DisableSchedule;
import io.digdag.cli.client.Download;
import io.digdag.cli.client.EnableSchedule;
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
import io.digdag.cli.client.ShowProjects;
import io.digdag.cli.client.Start;
import io.digdag.cli.client.Upload;
import io.digdag.cli.client.Version;
import io.digdag.core.Environment;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static io.digdag.cli.ConfigUtil.defaultConfigPath;
import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.client.DigdagVersion.buildVersion;
import static io.digdag.core.agent.OperatorManager.formatExceptionMessage;

public class Main
{
    private static final String DEFAULT_PROGRAM_NAME = "digdag";

    private final io.digdag.client.Version version;
    private final Map<String, String> env;
    private final PrintStream out;
    private final PrintStream err;
    private final InputStream in;
    private final String programName;

    public Main(io.digdag.client.Version version, Map<String, String> env, PrintStream out, PrintStream err, InputStream in)
    {
        this.version = version;
        this.env = env;
        this.out = out;
        this.err = err;
        this.in = in;
        this.programName = System.getProperty("io.digdag.cli.programName", DEFAULT_PROGRAM_NAME);
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

    protected void addCommands(final JCommander jc, final Injector injector)
    {
        jc.addCommand("init", injector.getInstance(Init.class), "new");
        jc.addCommand("run", injector.getInstance(Run.class), "r");
        jc.addCommand("check", injector.getInstance(Check.class), "c");
        jc.addCommand("scheduler", injector.getInstance(Sched.class), "sched");

        jc.addCommand("server", injector.getInstance(Server.class));

        jc.addCommand("push", injector.getInstance(Push.class));
        jc.addCommand("archive", injector.getInstance(Archive.class));
        jc.addCommand("upload", injector.getInstance(Upload.class));
        jc.addCommand("download", injector.getInstance(Download.class));

        jc.addCommand("project", injector.getInstance(ShowProjects.class), "projects");
        jc.addCommand("workflow", injector.getInstance(ShowWorkflow.class), "workflows");
        jc.addCommand("start", injector.getInstance(Start.class));
        jc.addCommand("retry", injector.getInstance(Retry.class));
        jc.addCommand("session", injector.getInstance(ShowSession.class), "sessions");
        jc.addCommand("attempts", injector.getInstance(ShowAttempts.class));
        jc.addCommand("attempt", injector.getInstance(ShowAttempt.class));
        jc.addCommand("reschedule", injector.getInstance(Reschedule.class));
        jc.addCommand("backfill", injector.getInstance(Backfill.class));
        jc.addCommand("log", injector.getInstance(ShowLog.class), "logs");
        jc.addCommand("kill", injector.getInstance(Kill.class));
        jc.addCommand("task", injector.getInstance(ShowTask.class), "tasks");
        jc.addCommand("schedule", injector.getInstance(ShowSchedule.class), "schedules");
        jc.addCommand("disable", injector.getInstance(DisableSchedule.class));
        jc.addCommand("enable", injector.getInstance(EnableSchedule.class));
        jc.addCommand("delete", injector.getInstance(Delete.class));
        jc.addCommand("secrets", injector.getInstance(Secrets.class), "secret");
        jc.addCommand("version", injector.getInstance(Version.class), "version");
        jc.addCommand("migrate", injector.getInstance(Migrate.class));
        jc.addCommand("profile", injector.getInstance(Profile.class));

        jc.addCommand("selfupdate", injector.getInstance(SelfUpdate.class));
    }

    public int cli(String... args)
    {
        for (String arg : args) {
            if ("--version".equals(arg)) {
                out.println(version.toString());
                return 0;
            }
        }
        err.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").format(new Date()) + ": Digdag v" + version);
        if (args.length == 0) {
            usage(null);
            return 0;
        }

        boolean verbose = false;

        MainOptions mainOpts = new MainOptions();
        JCommander jc = new JCommander(mainOpts);
        jc.addConverterFactory(new IdConverterFactory());
        jc.setProgramName(programName);

        // TODO: Use a pojo instead to avoid guice overhead
        Injector injector = Guice.createInjector(new AbstractModule()
        {
            @Override
            protected void configure()
            {
                bind(new TypeLiteral<Map<String, String>>() {}).annotatedWith(Environment.class).toInstance(env);
                bind(io.digdag.client.Version.class).toInstance(version);
                bind(String.class).annotatedWith(ProgramName.class).toInstance(programName);
                bind(InputStream.class).annotatedWith(StdIn.class).toInstance(in);
                bind(PrintStream.class).annotatedWith(StdOut.class).toInstance(out);
                bind(PrintStream.class).annotatedWith(StdErr.class).toInstance(err);
            }
        });

        addCommands(jc, injector);

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
            String message = formatExceptionMessage(ex);
            if (message.trim().isEmpty()) {
                // prevent silent crash
                ex.printStackTrace(err);
            }
            else {
                err.println("error: " + message);
                if (verbose) {
                    ex.printStackTrace(err);
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
        err.println("Usage: " + programName + " <command> [options...]");
        err.println("  Local-mode commands:");
        err.println("    init <dir>                         create a new workflow project");
        err.println("    r[un] <workflow.dig>               run a workflow");
        err.println("    c[heck]                            show workflow definitions");
        err.println("    sched[uler]                        run a scheduler server");
        err.println("    migrate (run|check)                migrate database");
        err.println("    profile                            profile archived information");
        err.println("    selfupdate                         update cli to the latest version");
        err.println("");
        err.println("  Server-mode commands:");
        err.println("    server                             start server");
        err.println("");
        err.println("  Client-mode commands:");
        err.println("    push <project-name>                create and upload a new revision");
        err.println("    download <project-name>            pull an uploaded revision");
        err.println("    start <project-name> <name>        start a new session attempt of a workflow");
        err.println("    retry <attempt-id>                 retry a session");
        err.println("    kill <attempt-id>                  kill a running session attempt");
        err.println("    backfill <schedule-id>             start sessions of a schedule for past times");
        err.println("    backfill <project-name> <name>     start sessions of a schedule for past times");
        err.println("    reschedule <schedule-id>           skip sessions of a schedule to a future time");
        err.println("    reschedule <project-name> <name>   skip sessions of a schedule to a future time");
        err.println("    log <attempt-id>                   show logs of a session attempt");
        err.println("    projects [name]                    show projects");
        err.println("    workflows [project-name] [name]    show registered workflow definitions");
        err.println("    schedules                          show registered schedules");
        err.println("    disable <schedule-id>              disable a workflow schedule");
        err.println("    disable <project-name>             disable all workflow schedules in a project");
        err.println("    disable <project-name> <name>      disable a workflow schedule");
        err.println("    enable <schedule-id>               enable a workflow schedule");
        err.println("    enable <project-name>              enable all workflow schedules in a project");
        err.println("    enable <project-name> <name>       enable a workflow schedule");
        err.println("    sessions                           show sessions for all workflows");
        err.println("    sessions <project-name>            show sessions for all workflows in a project");
        err.println("    sessions <project-name> <name>     show sessions for a workflow");
        err.println("    session  <session-id>              show a single session");
        err.println("    attempts                           show attempts for all sessions");
        err.println("    attempts <session-id>              show attempts for a session");
        err.println("    attempt  <attempt-id>              show a single attempt");
        err.println("    tasks <attempt-id>                 show tasks of a session attempt");
        err.println("    delete <project-name>              delete a project");
        err.println("    secrets --project <project-name>   manage secrets");
        err.println("    version                            show client and server version");
        err.println("");
        err.println("  Options:");
        showCommonOptions(env, err);
        err.println("  Client options:");
        showClientCommonOptions(err);
        if (error == null) {
            err.println("Use `<command> --help` to see detailed usage of a command.");
            return systemExit(null);
        }
        else {
            return systemExit(error);
        }
    }

    public static void showCommonOptions(Map<String, String> env, PrintStream err)
    {
        err.println("    -L, --log PATH                   output log messages to a file (default: -)");
        err.println("    -l, --log-level LEVEL            log level (error, warn, info, debug or trace)");
        err.println("    -X KEY=VALUE                     add a performance system config");
        err.println("    -c, --config PATH.properties     Configuration file (default: " + defaultConfigPath(env) + ")");
        err.println("    --version                        show client version");
        err.println("");
    }

    public static void showClientCommonOptions(PrintStream err)
    {
        err.println("    -e, --endpoint URL               Server endpoint (default: http://127.0.0.1:65432)");
        err.println("    -H, --header  KEY=VALUE          Additional headers");
        err.println("    --disable-version-check          Disable server version check");
        err.println("    --disable-cert-validation        Disable certificate verification");
        err.println("");
    }

}
