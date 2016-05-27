package io.digdag.cli;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.time.Instant;
import java.time.ZoneId;

import org.hjson.JsonValue;
import org.hjson.Stringify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.DynamicParameter;
import com.google.common.base.Optional;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.repository.Revision;
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinitionList;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.workflow.Workflow;
import io.digdag.core.workflow.WorkflowTask;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.config.ConfigLoaderManager;
import io.digdag.spi.Scheduler;
import io.digdag.spi.ScheduleTime;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import static io.digdag.cli.TimeUtil.formatTime;
import static io.digdag.cli.TimeUtil.formatTimeDiff;
import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.Arguments.loadProject;
import static io.digdag.cli.Arguments.normalizeWorkflowName;
import static io.digdag.cli.SystemExitException.systemExit;

public class Check
    extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(Check.class);

    @Parameter(names = {"--project"})
    String projectDirName = null;

    @DynamicParameter(names = {"-p", "--param"})
    Map<String, String> params = new HashMap<>();

    @Parameter(names = {"-P", "--params-file"})
    String paramsFile = null;

    //@Parameter(names = {"-G", "--graph"})
    //String visualizePath = null;

    public Check(PrintStream out, PrintStream err)
    {
        super(out, err);
    }

    @Override
    public void main()
            throws Exception
    {
        switch (args.size()) {
        case 0:
            check(null);
            break;
        case 1:
            check(args.get(0));
            break;
        default:
            throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag check [workflow.dig] [options...]");
        err.println("  Options:");
        err.println("        --project DIR                use this directory as the project directory (default: current directory)");
        err.println("    -p, --param KEY=VALUE            overwrite a parameter (use multiple times to set many parameters)");
        err.println("    -P, --params-file PATH.yml       read parameters from a YAML file");
        //err.println("    -g, --graph OUTPUT.png           visualize a task and exit");
        Main.showCommonOptions(err);
        return systemExit(error);
    }

    public void check(String workflowNameArg) throws Exception
    {
        Injector injector = new DigdagEmbed.Bootstrap()
            .withWorkflowExecutor(false)
            .withScheduleExecutor(false)
            .withLocalAgent(false)
            .initialize()
            .getInjector();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);
        final ProjectArchiveLoader projectLoader = injector.getInstance(ProjectArchiveLoader.class);

        Config overwriteParams = loadParams(cf, loader, loadSystemProperties(), paramsFile, params);

        showSystemDefaults();

        ProjectArchive project = loadProject(projectLoader, projectDirName, overwriteParams);

        Optional<String> onlyWorkflow = Optional.fromNullable(workflowNameArg).transform(it -> normalizeWorkflowName(project, it));

        showProject(injector, project, onlyWorkflow);
    }

    private void showSystemDefaults()
    {
        ln("  System default timezone: %s",
                ZoneId.systemDefault());
        ln("");
    }

    private void showProject(Injector injector, ProjectArchive project, Optional<String> onlyWorkflow)
    {
        final WorkflowCompiler compiler = injector.getInstance(WorkflowCompiler.class);
        final SchedulerManager schedulerManager = injector.getInstance(SchedulerManager.class);

        ArchiveMetadata meta = project.getArchiveMetadata();

        Revision rev = Revision.builderFromArchive("check", meta)
            .archiveType("null")
            .build();

        WorkflowDefinitionList defs = meta.getWorkflowList();

        {
            Formatter f = new Formatter("    ");
            for (WorkflowDefinition def : defs.get()) {
                Workflow wf = compiler.compile(def.getName(), def.getConfig());
                // TODO validate workflow and schedule
                f.ln("%s (%d tasks)", def.getName(), wf.getTasks().size());
                Set<String> required = new HashSet<>();
                for (WorkflowTask task : wf.getTasks()) {
                    Config config = task.getConfig();
                    String require = config.getOptional("require>", String.class).orNull();
                    if (require != null && required.add(require)) {
                        f.ln("  -> %s", require);
                    }
                }
            }
            ln("  Definitions (%d workflows):", defs.get().size());
            f.print();
            ln("");
        }

        {
            ln("  Parameters:");
            Formatter f = new Formatter("    ");
            f.ln(JsonValue.readJSON(rev.getDefaultParams().toString()).toString(Stringify.HJSON));
            f.print();
            ln("");
        }

        {
            Formatter f = new Formatter("    ");
            int count = 0;
            for (WorkflowDefinition def : defs.get()) {
                Optional<Scheduler> sr = schedulerManager.tryGetScheduler(rev, def);
                if (sr.isPresent()) {
                    showSchedule(f, rev, sr.get(), def);
                    count++;
                }
            }
            ln("  Schedules (%d entries):", count);
            f.print();
            ln("");
        }
    }

    private static void showSchedule(
            Formatter f, Revision rev,
            Scheduler sr, WorkflowDefinition def)
    {
        Config schedConfig = SchedulerManager.getScheduleConfig(def);

        Instant now = Instant.now();
        ScheduleTime firstTime = sr.getFirstScheduleTime(now);

        f.ln("%s:", def.getName());
        f.indent = "      ";
        f.ln(JsonValue.readJSON(schedConfig.toString()).toString(Stringify.HJSON));
        f.ln("first session time: %s", TimeUtil.formatTime(firstTime.getTime()));
        f.ln("first runs at: %s (%s later)", TimeUtil.formatTime(firstTime.getRunTime()), formatTimeDiff(now, firstTime.getRunTime()));
        f.indent = "    ";
    }

    private void ln(String format, Object... args)
    {
        out.println(String.format(format, args));
    }

    private class Formatter
    {
        private StringBuilder sb = new StringBuilder();
        String indent;

        public Formatter(String indent)
        {
            this.indent = indent;
        }

        private void ln(String format, Object... args)
        {
            String string;
            if (args.length == 0) {
                string = format;
            }
            else {
                string = String.format(format, args);
            }
            for (String line : string.split("\n")) {
                sb.append(indent);
                sb.append(line);
                sb.append(String.format("%n"));
            }
        }

        public void print()
        {
            out.print(sb.toString());
        }
    }
}
