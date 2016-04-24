package io.digdag.cli;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;

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
import static io.digdag.cli.client.ClientCommand.formatTime;
import static io.digdag.cli.client.ClientCommand.formatTimeDiff;
import static io.digdag.cli.Arguments.loadParams;
import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.cli.Run.DEFAULT_DAGFILE;

public class Check
    extends Command
{
    private static final Logger logger = LoggerFactory.getLogger(Check.class);

    @Parameter(names = {"-f", "--file"})
    String dagfilePath = null;

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
        if (dagfilePath == null) {
            dagfilePath = DEFAULT_DAGFILE;
        }

        check();
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: digdag check [options...]");
        err.println("  Options:");
        err.println("    -f, --file PATH                  use this file to load tasks (default: digdag.yml)");
        err.println("    -p, --param KEY=VALUE            overwrite a parameter (use multiple times to set many parameters)");
        err.println("    -P, --params-file PATH.yml       read parameters from a YAML file");
        //err.println("    -g, --graph OUTPUT.png           visualize a task and exit");
        Main.showCommonOptions(err);
        return systemExit(error);
    }

    public void check() throws Exception
    {
        Injector injector = new DigdagEmbed.Bootstrap()
            .withWorkflowExecutor(false)
            .withScheduleExecutor(false)
            .withLocalAgent(false)
            .addModules(binder -> {
                binder.bind(YamlMapper.class).in(Scopes.SINGLETON);
            })
            .initialize()
            .getInjector();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ConfigLoaderManager loader = injector.getInstance(ConfigLoaderManager.class);
        final ProjectArchiveLoader projectLoader = injector.getInstance(ProjectArchiveLoader.class);

        Config overwriteParams = loadParams(cf, loader, loadSystemProperties(), paramsFile, params);

        showSystemDefaults();

        ProjectArchive project = projectLoader.loadProjectOrSingleWorkflow(Paths.get(dagfilePath), overwriteParams);

        showProject(injector, project);
    }

    public void showSystemDefaults()
    {
        ln("  System default timezone: %s",
                ZoneId.systemDefault());
        ln("");
    }

    public void showProject(Injector injector, ProjectArchive project)
    {
        final YamlMapper yamlMapper = injector.getInstance(YamlMapper.class);
        final WorkflowCompiler compiler = injector.getInstance(WorkflowCompiler.class);
        final SchedulerManager schedulerManager = injector.getInstance(SchedulerManager.class);

        ArchiveMetadata meta = project.getMetadata();

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
            f.ln(yamlMapper.toYaml(rev.getDefaultParams()));
            f.print();
            ln("");
        }

        {
            Formatter f = new Formatter("    ");
            int count = 0;
            for (WorkflowDefinition def : defs.get()) {
                Optional<Scheduler> sr = schedulerManager.tryGetScheduler(rev, def);
                if (sr.isPresent()) {
                    showSchedule(yamlMapper, f, rev, sr.get(), def);
                    count++;
                }
            }
            ln("  Schedules (%d entries):", count);
            f.print();
            ln("");
        }
    }

    private static void showSchedule(
            YamlMapper yamlMapper,
            Formatter f, Revision rev,
            Scheduler sr, WorkflowDefinition def)
    {
        Config schedConfig = SchedulerManager.getScheduleConfig(def);

        Instant now = Instant.now();
        ScheduleTime firstTime = sr.getFirstScheduleTime(now);

        f.ln("%s:", def.getName());
        f.indent = "      ";
        f.ln(yamlMapper.toYaml(schedConfig));
        f.ln("first session time: %s", formatTime(firstTime.getTime()));
        f.ln("first runs at: %s (%s later)", formatTime(firstTime.getRunTime()), formatTimeDiff(now, firstTime.getRunTime()));
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
