package io.digdag.core;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.time.Instant;
import java.time.ZoneId;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.agent.LocalAgentManager;
import io.digdag.core.database.DatabaseMigrator;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.ScheduleExecutor;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.session.*;
import io.digdag.core.workflow.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.workflow.TaskMatchPattern.MultipleTaskMatchException;
import io.digdag.core.workflow.TaskMatchPattern.NoMatchException;
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
    private final SchedulerManager srm;
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
            SchedulerManager srm,
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
        this.srm = srm;
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
        private final List<StoredWorkflowDefinition> workflows;

        public StoreWorkflow(StoredRevision revision, List<StoredWorkflowDefinition> workflows)
        {
            this.revision = revision;
            this.workflows = workflows;
        }

        public StoredRevision getRevision()
        {
            return revision;
        }

        public List<StoredWorkflowDefinition> getWorkflows()
        {
            return workflows;
        }
    }

    private StoreWorkflow storeLocalWorkflowsImpl(String repositoryName, Revision revision,
            WorkflowDefinitionList defs,
            Optional<Instant> currentTimeToSchedule)
        throws ResourceConflictException, ResourceNotFoundException
    {
        // validate workflow
        // TODO move this to RepositoryControl
        defs.get()
            .stream()
            .forEach(workflowSource -> compiler.compile(workflowSource.getName(), workflowSource.getConfig()));

        return repoStore.putAndLockRepository(
                Repository.of(repositoryName),
                (store, storedRepo) -> {
                    RepositoryControl lockedRepo = new RepositoryControl(store, storedRepo);
                    StoredRevision rev = lockedRepo.putRevision(revision);
                    List<StoredWorkflowDefinition> storedDefs;
                    if (currentTimeToSchedule.isPresent()) {
                        storedDefs = lockedRepo.insertWorkflowDefinitions(rev, defs.get(), srm, currentTimeToSchedule.get());
                    }
                    else {
                        storedDefs = lockedRepo.insertWorkflowDefinitionsWithoutSchedules(rev, defs.get());
                    }
                    return new StoreWorkflow(rev, storedDefs);
                });
    }

    private StoreWorkflow storeLocalWorkflows(
            String revisionName,
            WorkflowDefinitionList defs,
            Optional<Instant> currentTimeToSchedule,
            Config defaultParams)
        throws ResourceConflictException, ResourceNotFoundException
    {
        return storeLocalWorkflowsImpl(
                "default",
                Revision.revisionBuilder()
                    .name(revisionName)
                    .archiveType("db")
                    .defaultParams(defaultParams)
                    .build(),
                defs,
                currentTimeToSchedule);
    }

    public StoredSessionAttemptWithSession storeAndStartWorkflows(
            ZoneId defaultTimeZone,
            WorkflowDefinitionList defs,
            TaskMatchPattern taskMatchPattern,
            Config overwriteParams)
        throws ResourceConflictException, ResourceNotFoundException, SessionAttemptConflictException
    {
        StoreWorkflow revWfs = storeLocalWorkflows("revision", defs,
                Optional.absent(), overwriteParams);
        final StoredRevision revision = revWfs.getRevision();
        final List<StoredWorkflowDefinition> sources = revWfs.getWorkflows();

        try {
            StoredWorkflowDefinition def = taskMatchPattern.findRootWorkflow(sources);

            AttemptRequest ar = AttemptRequest.builder()
                .repositoryId(revision.getRepositoryId())
                .workflowName(def.getName())
                .instant(Instant.now())
                .retryAttemptName(Optional.absent())
                .defaultTimeZone(defaultTimeZone)
                .defaultParams(cf.create())
                .overwriteParams(overwriteParams)
                .build();

            if (taskMatchPattern.getSubtaskMatchPattern().isPresent()) {
                return exec.submitSubworkflow(0, ar, def, taskMatchPattern.getSubtaskMatchPattern().get(), ImmutableList.of());
            }
            else {
                return exec.submitWorkflow(0, ar, def, ImmutableList.of());
            }
        }
        catch (NoMatchException ex) {
            //logger.error("No task matched with '{}'", fromTaskName.orNull());
            throw new IllegalArgumentException(ex);  // TODO exception class
        }
        catch (MultipleTaskMatchException ex) {
            throw new IllegalArgumentException(ex);  // TODO exception class
        }
    }

    public StoredRevision storeWorkflows(
            String revisionName,
            WorkflowDefinitionList defs,
            Instant currentTime,
            Config defaultParams)
        throws ResourceConflictException, ResourceNotFoundException
    {
        return storeLocalWorkflows(
                revisionName,
                defs,
                Optional.of(currentTime),
                defaultParams)
            .getRevision();
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
