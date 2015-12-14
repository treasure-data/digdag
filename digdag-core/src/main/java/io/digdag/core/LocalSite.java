package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Collectors;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.agent.LocalAgentManager;
import io.digdag.core.database.DatabaseMigrator;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.ScheduleExecutor;
import io.digdag.core.schedule.ScheduleStoreManager;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.session.*;
import io.digdag.core.workflow.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.config.Config;
import io.digdag.spi.config.ConfigFactory;
import io.digdag.core.yaml.YamlConfigLoader;

public class LocalSite
{
    private static Logger logger = LoggerFactory.getLogger(LocalSite.class);

    private final ConfigFactory cf;
    private final YamlConfigLoader loader;
    private final WorkflowCompiler compiler;
    private final RepositoryStore repoStore;
    private final SessionStoreManager sessionStoreManager;
    private final SessionStore sessionStore;
    private final WorkflowExecutor exec;
    private final TaskQueueDispatcher dispatcher;
    private final LocalAgentManager localAgentManager;
    private final DatabaseMigrator databaseMigrator;
    private final ScheduleStoreManager scheduleStoreManager;
    private final SchedulerManager scheds;
    private final ScheduleExecutor scheduleExecutor;
    private final SessionMonitorExecutor sessionMonitorExecutor;
    private boolean schedulerStarted;

    @Inject
    public LocalSite(
            ConfigFactory cf,
            YamlConfigLoader loader,
            WorkflowCompiler compiler,
            RepositoryStoreManager repoStoreManager,
            SessionStoreManager sessionStoreManager,
            WorkflowExecutor exec,
            TaskQueueDispatcher dispatcher,
            LocalAgentManager localAgentManager,
            DatabaseMigrator databaseMigrator,
            ScheduleStoreManager scheduleStoreManager,
            SchedulerManager scheds,
            ScheduleExecutor scheduleExecutor,
            SessionMonitorExecutor sessionMonitorExecutor)
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
        this.scheduleStoreManager = scheduleStoreManager;
        this.scheds = scheds;
        this.scheduleExecutor = scheduleExecutor;
        this.sessionMonitorExecutor = sessionMonitorExecutor;
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

    public void startMonitor()
    {
        sessionMonitorExecutor.start();
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
                        .map(workflowSource -> {
                            try {
                                return repoControl.insertWorkflow(rev.getId(), workflowSource);
                            }
                            catch (ResourceConflictException ex) {
                                throw new IllegalStateException("Database state error", ex);
                            }
                        })
                        .collect(Collectors.toList());
                    if (currentTimeToSchedule.isPresent()) {
                        try {
                            repoControl.syncSchedulesTo(scheduleStoreManager, scheds,
                                    currentTimeToSchedule.get(), rev);
                        }
                        catch (ResourceConflictException ex) {
                            throw new IllegalStateException("Database state error", ex);
                        }
                    }
                    return new StoreWorkflow(rev, storedWorkflows);
                });
    }

    private StoreWorkflow storeWorkflows(List<WorkflowSource> workflowSources,
            Optional<Date> currentTimeToSchedule)
    {
        return storeWorkflows(
                "default",
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
            Optional<String> fromTaskName,
            Config sessionParams,
            SessionOptions options,
            Date slaCurrentTime)
    {
        StoreWorkflow revWfs = storeWorkflows(workflowSources, Optional.absent());
        final StoredRevision revision = revWfs.getRevision();
        final List<StoredWorkflowSource> workflows = revWfs.getWorkflows();

        final Session trigger = Session.sessionBuilder()
            .name("session-" + UUID.randomUUID().toString())
            .params(sessionParams)
            .options(options)
            .build();

        return workflows.stream()
            .map(workflow -> {
                try {
                    SessionRelation rel = SessionRelation.ofWorkflow(revision.getRepositoryId(), revision.getId(), workflow.getId());
                    return exec.submitWorkflow(0, workflow, trigger, Optional.of(rel), slaCurrentTime,
                            fromTaskName.transform(name -> new TaskMatchPattern(name)));
                }
                catch (TaskMatchPattern.NoMatchException ex) {
                    logger.error("No task matched with '{}'", fromTaskName.orNull());
                    return null;
                }
                catch (TaskMatchPattern.MultipleMatchException ex) {
                    throw new IllegalArgumentException(ex);  // TODO exception class
                }
                catch (ResourceConflictException ex) {
                    throw new IllegalStateException("UUID confliction", ex);
                }
            })
            .filter(wf -> wf != null)
            .collect(Collectors.toList());
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
