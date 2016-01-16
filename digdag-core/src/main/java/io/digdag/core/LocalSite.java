package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.util.UUID;
import java.util.TimeZone;
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
    private final SessionMonitorManager sessionMonitorManager;
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
            SessionMonitorManager sessionMonitorManager,
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
        this.sessionMonitorManager = sessionMonitorManager;
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
        private final List<StoredScheduleSource> schedules;

        public StoreWorkflow(StoredRevision revision, List<StoredWorkflowSource> workflows, List<StoredScheduleSource> schedules)
        {
            this.revision = revision;
            this.workflows = workflows;
            this.schedules = schedules;
        }

        public StoredRevision getRevision()
        {
            return revision;
        }

        public List<StoredWorkflowSource> getWorkflows()
        {
            return workflows;
        }

        public List<StoredScheduleSource> getSchedules()
        {
            return schedules;
        }
    }

    private StoreWorkflow storeWorkflows(String repositoryName, Revision revision,
            WorkflowSourceList workflowSources, ScheduleSourceList scheduleSources,
            Optional<Date> currentTimeToSchedule)
    {
        // validate workflow
        // TODO move this to RepositoryControl
        workflowSources.get()
            .stream()
            .forEach(workflowSource -> compiler.compile(workflowSource.getName(), workflowSource.getConfig()));

        return repoStore.putRepository(
                Repository.of(repositoryName),
                (repoControl) -> {
                    StoredRevision rev = repoControl.putRevision(revision);
                    try {
                        List<StoredWorkflowSource> storedWorkflows =
                            repoControl.insertWorkflowSources(rev.getId(), workflowSources.get());
                        List<StoredScheduleSource> storedSchedules =
                            repoControl.insertScheduleSources(rev.getId(), scheduleSources.get());
                        if (currentTimeToSchedule.isPresent()) {
                            repoControl.syncSchedules(
                                    scheduleStoreManager, scheds,
                                    rev, storedWorkflows, storedSchedules,
                                    currentTimeToSchedule.get());
                        }
                        return new StoreWorkflow(rev, storedWorkflows, storedSchedules);
                    }
                    catch (ResourceConflictException ex) {
                        throw new IllegalStateException("Database state error", ex);
                    }
                });
    }

    private StoreWorkflow storeWorkflows(
            WorkflowSourceList workflowSources,
            ScheduleSourceList scheduleSources,
            Optional<Date> currentTimeToSchedule)
    {
        return storeWorkflows(
                "default",
                Revision.revisionBuilder()
                    .name("revision")
                    .archiveType("db")
                    .defaultParams(cf.create())
                    .build(),
                workflowSources,
                scheduleSources,
                currentTimeToSchedule);
    }

    public List<StoredSession> startWorkflows(
            TimeZone defaultTimeZone,
            WorkflowSourceList workflowSources,
            Optional<String> fromTaskName,
            Config overwriteParams,
            Date slaCurrentTime,
            SessionOptions options)
    {
        StoreWorkflow revWfs = storeWorkflows(workflowSources,
                ScheduleSourceList.of(ImmutableList.of()), Optional.absent());
        final StoredRevision revision = revWfs.getRevision();
        final List<StoredWorkflowSource> workflows = revWfs.getWorkflows();

        return workflows.stream()
            .map(workflow -> {
                try {
                    Session trigger = Session.sessionBuilder(
                            "session-" + UUID.randomUUID().toString(),
                            defaultTimeZone,
                            cf.create(), workflow, overwriteParams)
                        .options(options)
                        .build();

                    SessionRelation rel = SessionRelation.ofWorkflow(revision.getRepositoryId(), revision.getId(), workflow.getId());
                    return exec.submitWorkflow(0, workflow, trigger, Optional.of(rel),
                            slaCurrentTime, fromTaskName.transform(name -> new TaskMatchPattern(name)));
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
            WorkflowSourceList workflowSources,
            ScheduleSourceList scheduleSources,
            Date currentTime)
    {
        storeWorkflows(workflowSources, scheduleSources, Optional.of(currentTime));
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
