package io.digdag.cli;

import java.util.List;
import java.util.stream.Collectors;
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
import static io.digdag.cli.Main.systemExit;
import static java.util.Arrays.asList;

public class Run
{
    private static Logger logger = LoggerFactory.getLogger(Run.class);

    public static void main(String command, String[] args)
            throws Exception
    {
        OptionParser parser = Main.parser();

        parser.acceptsAll(asList("r", "resume-state")).withRequiredArg().ofType(String.class);
        parser.acceptsAll(asList("s", "show")).withRequiredArg().ofType(String.class);

        OptionSet op = Main.parse(parser, args);
        List<String> argv = Main.nonOptions(op);
        if (op.has("help") || argv.size() != 1) {
            throw usage(null);
        }

        Optional<File> visualizePath = Optional.fromNullable((String) op.valueOf("s")).transform(it -> new File(it));
        Optional<File> resumeStateFilePath = Optional.fromNullable((String) op.valueOf("r")).transform(it -> new File(it));
        File workflowPath = new File(argv.get(0));

        new Run(workflowPath, resumeStateFilePath, visualizePath).run();
    }

    private static SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag run <workflow.yml> [options...]");
        System.err.println("  Options:");
        System.err.println("    -r, --resume-state PATH.yml      path to resume state file");
        System.err.println("    -s, --show PATH.png              visualize result of execution and create a PNG file");
        // TODO add -p, --param K=V
        // TODO add -P, --params-file PATH.yml
        Main.showCommonOptions();
        System.err.println("");
        return systemExit(error);
    }

    static List<WorkflowSource> loadWorkflowSources(final ConfigSource ast)
    {
        return ast.getKeys().stream()
            .map(key -> WorkflowSource.of(key, ast.getNested(key)))
            .collect(Collectors.toList());
    }

    static WorkflowSource loadFirstWorkflowSource(final ConfigSource ast)
    {
        List<WorkflowSource> workflowSources = loadWorkflowSources(ast);
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
    private final Optional<File> resumeStateFilePath;
    private final Optional<File> visualizePath;

    public Run(File workflowPath, Optional<File> resumeStateFilePath, Optional<File> visualizePath)
    {
        this.workflowPath = workflowPath;
        this.resumeStateFilePath = resumeStateFilePath;
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

        final ConfigSourceFactory cf = injector.getInstance(ConfigSourceFactory.class);
        final YamlConfigLoader loader = injector.getInstance(YamlConfigLoader.class);
        final FileMapper mapper = injector.getInstance(FileMapper.class);
        final ResumeStateFileManager rsm = injector.getInstance(ResumeStateFileManager.class);

        Optional<ResumeState> resumeState = Optional.absent();
        if (resumeStateFilePath.isPresent() && mapper.checkExists(resumeStateFilePath.get())) {
            resumeState = Optional.of(mapper.readFile(resumeStateFilePath.get(), ResumeState.class));
        }

        WorkflowSource workflowSource = loadFirstWorkflowSource(loader.loadFile(workflowPath));

        SessionOptions options = SessionOptions.builder()
            .skipTaskMap(resumeState.transform(it -> it.getReports()).or(ImmutableMap.of()))
            .build();

        StoredSession session = localSite.startWorkflows(
                ImmutableList.of(workflowSource),
                cf.create(), options).get(0);

        localSite.startLocalAgent();

        if (resumeStateFilePath.isPresent()) {
            rsm.startUpdate(resumeStateFilePath.get(), session);
        }

        localSite.runUntilAny();

        for (StoredTask task : localSite.getSessionStore().getAllTasks()) {
            logger.debug("  Task["+task.getId()+"]: "+task.getFullName());
            logger.debug("    parent: "+task.getParentId().transform(it -> Long.toString(it)).or("(root)"));
            logger.debug("    upstreams: "+task.getUpstreams().stream().map(it -> Long.toString(it)).collect(Collectors.joining(",")));
            logger.debug("    state: "+task.getState());
            logger.debug("    retryAt: "+task.getRetryAt());
            logger.debug("    config: "+task.getConfig());
            logger.debug("    taskType: "+task.getTaskType());
            logger.debug("    stateParams: "+task.getStateParams());
            logger.debug("    carryParams: "+task.getCarryParams());
            logger.debug("    report: "+task.getReport());
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
    }
}
