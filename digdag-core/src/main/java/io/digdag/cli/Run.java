package io.digdag.cli;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.util.stream.Collectors;
import io.digdag.core.repository.WorkflowSource;
import io.digdag.core.repository.WorkflowSourceList;
import io.digdag.core.workflow.SessionOptions;
import io.digdag.core.workflow.StoredSession;
import io.digdag.core.workflow.StoredTask;
import io.digdag.core.workflow.TaskStateCode;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import java.io.File;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.*;
import io.digdag.DigdagEmbed;
import io.digdag.cli.Main.SystemExitException;
import io.digdag.core.config.Config;
import io.digdag.core.config.ConfigFactory;
import static io.digdag.cli.Main.systemExit;
import static java.util.Arrays.asList;

public class Run
{
    private static Logger logger = LoggerFactory.getLogger(Run.class);

    public static void main(String command, String[] args)
            throws Exception
    {
        OptionParser parser = Main.parser();

        parser.acceptsAll(asList("f", "from")).withRequiredArg().ofType(String.class);
        parser.acceptsAll(asList("r", "resume-state")).withRequiredArg().ofType(String.class);
        parser.acceptsAll(asList("p", "param")).withRequiredArg().ofType(String.class);
        parser.acceptsAll(asList("P", "params-file")).withRequiredArg().ofType(String.class);
        parser.acceptsAll(asList("s", "show")).withRequiredArg().ofType(String.class);

        OptionSet op = Main.parse(parser, args);
        List<String> argv = Main.nonOptions(op);
        if (op.has("help") || argv.size() != 1) {
            throw usage(null);
        }

        Optional<String> fromTaskName = Optional.fromNullable((String) op.valueOf("f"));
        Optional<File> resumeStateFilePath = Optional.fromNullable((String) op.valueOf("r")).transform(it -> new File(it));
        Optional<File> paramsFilePath = Optional.fromNullable((String) op.valueOf("P")).transform(it -> new File(it));
        Optional<File> visualizePath = Optional.fromNullable((String) op.valueOf("s")).transform(it -> new File(it));
        File workflowPath = new File(argv.get(0));

        Map<String, String> params = new HashMap<>();
        for (Object kv : op.valuesOf("p")) {
            String[] pair = kv.toString().split("=", 2);
            String key = pair[0];
            String value = (pair.length > 1 ? pair[1] : "true");
            params.put(key, value);
        }

        new Run(workflowPath, fromTaskName, resumeStateFilePath, params, paramsFilePath, visualizePath).run();
    }

    private static SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag run <workflow.yml> [options...]");
        System.err.println("  Options:");
        System.err.println("    -f, --from +NAME                 skip tasks before this task");
        System.err.println("    -r, --resume-state PATH.yml      path to resume state file");
        System.err.println("    -p, --param KEY=VALUE            add a session parameter (use multiple times to set many parameters)");
        System.err.println("    -P, --params-file PATH.yml       read session parameters from a YAML file");
        System.err.println("    -s, --show PATH.png              visualize result of execution and create a PNG file");
        // TODO add -p, --param K=V
        Main.showCommonOptions();
        System.err.println("");
        return systemExit(error);
    }

    static WorkflowSource loadFirstWorkflowSource(final Config ast)
    {
        List<WorkflowSource> workflowSources = ast.convert(WorkflowSourceList.class).get();
        if (workflowSources.size() != 1) {
            if (workflowSources.isEmpty()) {
                throw new RuntimeException("Workflow file doesn't include definitions");
            }
            else {
                throw new RuntimeException("Workflow file includes more than one definitions");
            }
        }
        return workflowSources.get(0);
    }

    private final File workflowPath;
    private final Optional<String> fromTaskName;
    private final Optional<File> resumeStateFilePath;
    private final Map<String, String> params;
    private final Optional<File> paramsFilePath;
    private final Optional<File> visualizePath;

    public Run(File workflowPath,
            Optional<String> fromTaskName,
            Optional<File> resumeStateFilePath,
            Map<String, String> params,
            Optional<File> paramsFilePath,
            Optional<File> visualizePath)
    {
        this.workflowPath = workflowPath;
        this.fromTaskName = fromTaskName;
        this.resumeStateFilePath = resumeStateFilePath;
        this.params = params;
        this.paramsFilePath = paramsFilePath;
        this.visualizePath = visualizePath;
    }

    public void run() throws Exception
    {
        DigdagEmbed embed = Main.embed();
        try {
            run(embed.getInjector());
        }
        finally {
            // close explicitly so that ResumeStateFileManager.preDestroy runs before closing h2 database
            embed.destroy();
        }
    }

    public void run(Injector injector) throws Exception
    {
        LocalSite localSite = injector.getInstance(LocalSite.class);
        localSite.initialize();

        final ConfigFactory cf = injector.getInstance(ConfigFactory.class);
        final ArgumentConfigLoader loader = injector.getInstance(ArgumentConfigLoader.class);
        final FileMapper mapper = injector.getInstance(FileMapper.class);
        final ResumeStateFileManager rsm = injector.getInstance(ResumeStateFileManager.class);

        Config sessionParams = cf.create();
        if (paramsFilePath.isPresent()) {
            sessionParams.setAll(mapper.readFile(paramsFilePath.get(), Config.class));
        }
        for (Map.Entry<String, String> pair : params.entrySet()) {
            sessionParams.set(pair.getKey(), pair.getValue());
        }

        Optional<ResumeState> resumeState = Optional.absent();
        if (resumeStateFilePath.isPresent() && mapper.checkExists(resumeStateFilePath.get())) {
            resumeState = Optional.of(mapper.readFile(resumeStateFilePath.get(), ResumeState.class));
        }

        WorkflowSource workflowSource = loadFirstWorkflowSource(loader.load(workflowPath));

        SessionOptions options = SessionOptions.builder()
            .skipTaskMap(resumeState.transform(it -> it.getReports()).or(ImmutableMap.of()))
            .build();

        List<StoredSession> sessions = localSite.startWorkflows(
                ImmutableList.of(workflowSource),
                fromTaskName,
                sessionParams, options,
                new Date());
        if (sessions.isEmpty()) {
            throw systemExit("No workflows to start");
        }
        StoredSession session = sessions.get(0);
        logger.debug("Submitting {}", session);

        localSite.startLocalAgent();
        localSite.startMonitor();

        // if resumeStateFilePath is not set, use workflow.yml.resume.yml
        File resumeResultPath = resumeStateFilePath.or(new File(workflowPath.toString() + ".resume.yml"));
        rsm.startUpdate(resumeResultPath, session);

        localSite.runUntilAny();

        ArrayList<StoredTask> failedTasks = new ArrayList<>();
        for (StoredTask task : localSite.getSessionStore().getTasks(session.getId(), 100, Optional.absent())) {  // TODO paging
            if (task.getState() == TaskStateCode.ERROR) {
                failedTasks.add(task);
            }
            logger.debug("  Task["+task.getId()+"]: "+task.getFullName());
            logger.debug("    parent: "+task.getParentId().transform(it -> Long.toString(it)).or("(root)"));
            logger.debug("    upstreams: "+task.getUpstreams().stream().map(it -> Long.toString(it)).collect(Collectors.joining(",")));
            logger.debug("    state: "+task.getState());
            logger.debug("    retryAt: "+task.getRetryAt());
            logger.debug("    config: "+task.getConfig());
            logger.debug("    taskType: "+task.getTaskType());
            logger.debug("    stateParams: "+task.getStateParams());
            logger.debug("    carryParams: "+task.getReport().transform(report -> report.getCarryParams()).or(cf.create()));
            logger.debug("    in: "+task.getReport().transform(report -> report.getInputs()).or(ImmutableList.of()));
            logger.debug("    out: "+task.getReport().transform(report -> report.getOutputs()).or(ImmutableList.of()));
            logger.debug("    error: "+task.getError());
        }

        if (visualizePath.isPresent()) {
            Show.show(
                    localSite.getSessionStore().getTasks(session.getId(), 1024, Optional.absent())
                        .stream()
                        .map(it -> WorkflowVisualizerNode.of(it))
                        .collect(Collectors.toList()),
                    visualizePath.get());
        }

        if (!failedTasks.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (StoredTask task : failedTasks) {
                sb.append(String.format("Task %s failed.%n", task.getFullName()));
            }
            sb.append(String.format("Use `digdag run %s -r %s` to restart this workflow.",
                        workflowPath, resumeResultPath));
            throw systemExit(sb.toString());
        }
    }
}
