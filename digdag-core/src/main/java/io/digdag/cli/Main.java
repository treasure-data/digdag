package io.digdag.cli;

import java.io.File;
import java.util.stream.Collectors;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import java.util.List;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.*;

import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

public class Main
{
    public static void main(String[] args)
            throws Exception
    {
        configureLogging("DEBUG", "-");

        Injector injector = Guice.createInjector(
                new ObjectMapperModule()
                    .registerModule(new GuavaModule())
                    .registerModule(new JodaModule()),
                    new DatabaseModule(DatabaseStoreConfig.builder()
                        .type("h2")
                        //.url("jdbc:h2:../test")
                        .url("jdbc:h2:mem:test")
                        .build()),
                (binder) -> {
                    binder.bind(TaskApi.class).to(InProcessTaskApi.class).in(Scopes.SINGLETON);
                    binder.bind(ConfigSourceFactory.class).in(Scopes.SINGLETON);
                    binder.bind(ConfigSourceMapper.class).in(Scopes.SINGLETON);
                    binder.bind(DatabaseMigrator.class).in(Scopes.SINGLETON);
                    binder.bind(SessionExecutor.class).in(Scopes.SINGLETON);
                    binder.bind(YamlConfigLoader.class).in(Scopes.SINGLETON);
                    binder.bind(TaskQueueDispatcher.class).in(Scopes.SINGLETON);
                    binder.bind(LocalAgentManager.class).in(Scopes.SINGLETON);

                    Multibinder<TaskQueueFactory> taskQueueBinder = Multibinder.newSetBinder(binder, TaskQueueFactory.class);
                    taskQueueBinder.addBinding().to(MemoryTaskQueueFactory.class).in(Scopes.SINGLETON);

                    Multibinder<TaskExecutorFactory> taskExecutorBinder = Multibinder.newSetBinder(binder, TaskExecutorFactory.class);
                    taskExecutorBinder.addBinding().to(PyTaskExecutorFactory.class).in(Scopes.SINGLETON);
                    taskExecutorBinder.addBinding().to(ShTaskExecutorFactory.class).in(Scopes.SINGLETON);

                }
            );

        final ConfigSourceFactory cf = injector.getInstance(ConfigSourceFactory.class);
        final YamlConfigLoader loader = injector.getInstance(YamlConfigLoader.class);
        final WorkflowCompiler compiler = injector.getInstance(WorkflowCompiler.class);
        final RepositoryStore repoStore = injector.getInstance(DatabaseRepositoryStoreManager.class).getRepositoryStore(0);
        final SessionStore sessionStore = injector.getInstance(SessionStoreManager.class).getSessionStore(0);
        final SessionExecutor exec = injector.getInstance(SessionExecutor.class);
        final TaskQueueDispatcher dispatcher = injector.getInstance(TaskQueueDispatcher.class);

        injector.getInstance(DatabaseMigrator.class).migrate();

        injector.getInstance(LocalAgentManager.class).startLocalAgent(0, "local");

        String path;
        if (args.length > 0) {
            path = args[0];
        }
        else {
            path = "../demo.yml";
        }
        final ConfigSource ast = loader.loadFile(new File(path));
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
                    System.out.println("Starting a session of workflow "+storedWorkflow);

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
        exec.showTasks();

        new GraphvizWorkflowVisualizer()
            .visualize(
                    sessionStore.getTasks(sessions.get(0).getId(), 1024, Optional.absent())
                        .stream()
                        .map(it -> WorkflowVisualizerNode.of(it))
                        .collect(Collectors.toList()),
                    new File("graph.png"));
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

        org.slf4j.Logger logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        if (logger instanceof Logger) {
            ((Logger) logger).setLevel(Level.toLevel(level.toUpperCase(), Level.DEBUG));
        }
    }
}
