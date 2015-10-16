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

public class Main
{
    public static void main(String[] args)
            throws Exception
    {
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
    }
}
