package io.digdag.cli;

import java.util.List;
import java.util.stream.Collectors;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import java.io.File;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Injector;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.*;
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

        parser.acceptsAll(asList("s", "show")).withRequiredArg().ofType(String.class);

        OptionSet op = Main.parse(parser, args);
        List<String> argv = Main.nonOptions(op);
        if (op.has("help") || argv.size() != 1) {
            throw usage(null);
        }

        Optional<File> visualizePath = Optional.fromNullable((String) op.valueOf("s")).transform(it -> new File(it));
        File workflowPath = new File(argv.get(0));

        new Run().run(workflowPath, visualizePath);
    }

    private static SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag run <workflow.yml> [options...]");
        System.err.println("  Options:");
        System.err.println("    -s, --show PATH.png              visualize result of execution and create a PNG file");
        Main.showCommonOptions();
        System.err.println("");
        return systemExit(error);
    }

    public void run(File workflowPath, Optional<File> visualizePath)
            throws Exception
    {
        Injector injector = Main.embed().getInjector();

        final ConfigSourceFactory cf = injector.getInstance(ConfigSourceFactory.class);
        final YamlConfigLoader loader = injector.getInstance(YamlConfigLoader.class);
        final WorkflowCompiler compiler = injector.getInstance(WorkflowCompiler.class);
        final RepositoryStore repoStore = injector.getInstance(DatabaseRepositoryStoreManager.class).getRepositoryStore(0);
        final SessionStoreManager sessionStoreManager = injector.getInstance(SessionStoreManager.class);
        final SessionStore sessionStore = sessionStoreManager.getSessionStore(0);
        final SessionExecutor exec = injector.getInstance(SessionExecutor.class);
        final TaskQueueDispatcher dispatcher = injector.getInstance(TaskQueueDispatcher.class);

        injector.getInstance(DatabaseMigrator.class).migrate();

        injector.getInstance(LocalAgentManager.class).startLocalAgent(0, "local");

        final ConfigSource ast = loader.loadFile(workflowPath);
        List<WorkflowSource> workflowSources = ast.getKeys()
            .stream()
            .map(key -> WorkflowSource.of(key, ast.getNested(key)))
            .collect(Collectors.toList());

        final StoredRepository repo = repoStore.putRepository(
                Repository.of("repo1", cf.create()));
        final StoredRevision rev = repoStore.putRevision(
                repo.getId(),
                Revision.revisionBuilder()
                    .name("rev1")
                    .archiveType("db")
                    .globalParams(cf.create())
                    .build()
                );

        List<StoredWorkflowSource> storedWorkflows = workflowSources
            .stream()
            .map(workflowSource -> repoStore.putWorkflow(rev.getId(), workflowSource))
            .collect(Collectors.toList());

        List<Workflow> workflows = storedWorkflows
            .stream()
            .map(storedWorkflow -> compiler.compile(storedWorkflow.getName(), storedWorkflow.getConfig()))
            .collect(Collectors.toList());

        List<StoredSession> sessions = sessionStore.transaction(() ->
            storedWorkflows
                .stream()
                .map(storedWorkflow -> {
                    return exec.submitWorkflow(
                            0,
                            storedWorkflow,
                            Session.sessionBuilder()
                                .name("ses1")
                                .params(cf.create())
                                .options(SessionOptions.sessionOptionsBuilder().build())
                                .build(),
                            SessionRelation
                                .of(Optional.of(repo.getId()), Optional.of(storedWorkflow.getId())));
                })
                .collect(Collectors.toList())
        );

        exec.runUntilAny(dispatcher);
        for (StoredTask task : sessionStoreManager.getAllTasks()) {
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
                    sessionStore.getTasks(sessions.get(0).getId(), 1024, Optional.absent())
                        .stream()
                        .map(it -> WorkflowVisualizerNode.of(it))
                        .collect(Collectors.toList()),
                    visualizePath.get());
        }
    }
}
