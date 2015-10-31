package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.util.stream.Collectors;
import java.io.File;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalSite
{
    private final ConfigSourceFactory cf;
    private final YamlConfigLoader loader;
    private final WorkflowCompiler compiler;
    private final RepositoryStore repoStore;
    private final SessionStoreManager sessionStoreManager;
    private final SessionStore sessionStore;
    private final SessionExecutor exec;
    private final TaskQueueDispatcher dispatcher;
    private final LocalAgentManager localAgentManager;
    private final DatabaseMigrator databaseMigrator;
    private final ScheduleStore scheduleStore;
    private final SchedulerManager scheds;
    private final ScheduleExecutor scheduleExecutor;
    private final SlaExecutor slaExecutor;
    private boolean schedulerStarted;

    @Inject
    public LocalSite(
            ConfigSourceFactory cf,
            YamlConfigLoader loader,
            WorkflowCompiler compiler,
            RepositoryStoreManager repoStoreManager,
            SessionStoreManager sessionStoreManager,
            SessionExecutor exec,
            TaskQueueDispatcher dispatcher,
            LocalAgentManager localAgentManager,
            DatabaseMigrator databaseMigrator,
            ScheduleStoreManager scheduleStoreManager,
            SchedulerManager scheds,
            ScheduleExecutor scheduleExecutor,
            SlaExecutor slaExecutor)
    {
        this.cf = cf;
        this.loader = loader;
        this.compiler = compiler;
        this.repoStore = repoStoreManager.getRepositoryStore(0);
        this.sessionStoreManager = sessionStoreManager;
        this.sessionStore = sessionStoreManager.getSessionStore(0);
        this.exec = exec;
        this.dispatcher = dispatcher;
        this.localAgentManager = localAgentManager;
        this.databaseMigrator = databaseMigrator;
        this.scheduleStore = scheduleStoreManager.getScheduleStore(0);
        this.scheds = scheds;
        this.scheduleExecutor = scheduleExecutor;
        this.slaExecutor = slaExecutor;
    }

    public SessionStore getSessionStore()
    {
        return sessionStore;
    }

    public void initialize()
    {
        databaseMigrator.migrate();
    }

    public void startLocalAgent()
    {
        localAgentManager.startLocalAgent(0, "local");
    }

    public void startScheduler()
    {
        scheduleExecutor.start();
    }

    private class StoreWorkflow
    {
        private final StoredRevision revision;
        private final List<StoredWorkflowSource> workflows;

        public StoreWorkflow(StoredRevision revision, List<StoredWorkflowSource> workflows)
        {
            this.revision = revision;
            this.workflows = workflows;
        }

        public StoredRevision getRevision()
        {
            return revision;
        }

        public List<StoredWorkflowSource> getWorkflows()
        {
            return workflows;
        }
    }

    private StoreWorkflow storeWorkflows(String repositoryName, Revision revision,
            List<WorkflowSource> workflowSources, Optional<Date> currentTimeToSchedule)
    {
        // validate workflow
        // TODO move this to RepositoryControl
        workflowSources
            .stream()
            .forEach(workflowSource -> compiler.compile(workflowSource.getName(), workflowSource.getConfig()));

        return repoStore.putRepository(
                Repository.of(repositoryName),
                (repoControl) -> {
                    StoredRevision rev = repoControl.putRevision(revision);
                    List<StoredWorkflowSource> storedWorkflows =
                        workflowSources.stream()
                        .map(workflowSource -> repoControl.putWorkflow(rev.getId(), workflowSource))
                        .collect(Collectors.toList());
                    if (currentTimeToSchedule.isPresent()) {
                        repoControl.syncSchedulesTo(scheduleStore, scheds,
                                slaExecutor, currentTimeToSchedule.get(), rev);
                    }
                    return new StoreWorkflow(rev, storedWorkflows);
                });
    }

    private StoreWorkflow storeWorkflows(List<WorkflowSource> workflowSources,
            Optional<Date> currentTimeToSchedule)
    {
        return storeWorkflows(
                "repository",
                Revision.revisionBuilder()
                    .name("revision")
                    .archiveType("db")
                    .globalParams(cf.create())
                    .build(),
                workflowSources,
                currentTimeToSchedule);
    }

    public List<StoredSession> startWorkflows(
            List<WorkflowSource> workflowSources,
            ConfigSource sessionParams, SessionOptions options)
    {
        StoreWorkflow revWfs = storeWorkflows(workflowSources, Optional.absent());
        final StoredRevision revision = revWfs.getRevision();
        final List<StoredWorkflowSource> workflows = revWfs.getWorkflows();

        final Session trigger = Session.sessionBuilder()
            .name("session")
            .params(sessionParams)
            .options(options)
            .build();

        return sessionStore.transaction(() -> {
            return workflows.stream()
                .map(workflow -> {
                    return exec.submitWorkflow(0, workflow, trigger,
                            SessionNamespace.ofWorkflow(revision.getRepositoryId(), workflow.getId()));
                })
                .collect(Collectors.toList());
        });
    }

    public void scheduleWorkflows(
            List<WorkflowSource> workflowSources,
            Date currentTime)
    {
        storeWorkflows(workflowSources, Optional.of(currentTime));
    }

    public void run()
            throws InterruptedException
    {
        exec.run(dispatcher);
    }

    public void runUntilAny()
            throws InterruptedException
    {
        exec.runUntilAny(dispatcher);
    }
}
