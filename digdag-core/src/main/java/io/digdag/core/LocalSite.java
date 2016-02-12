package io.digdag.core;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.time.Instant;
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

public class LocalSite
{
    private static Logger logger = LoggerFactory.getLogger(LocalSite.class);

    private final ConfigFactory cf;
    private final WorkflowCompiler compiler;
    private final RepositoryStore repoStore;
    private final SessionStoreManager sessionStoreManager;
    private final SessionStore sessionStore;
    private final WorkflowExecutor exec;
    private final LocalAgentManager localAgentManager;
    private final DatabaseMigrator databaseMigrator;
    private final SchedulerManager srm;
    private final ScheduleExecutor scheduleExecutor;
    private final SessionMonitorExecutor sessionMonitorExecutor;
    private boolean schedulerStarted;

    @Inject
    public LocalSite(
            ConfigFactory cf,
            WorkflowCompiler compiler,
            RepositoryStoreManager repoStoreManager,
            SessionStoreManager sessionStoreManager,
            WorkflowExecutor exec,
            LocalAgentManager localAgentManager,
            DatabaseMigrator databaseMigrator,
            SchedulerManager srm,
            ScheduleExecutor scheduleExecutor,
            SessionMonitorExecutor sessionMonitorExecutor)
    {
        this.cf = cf;
        this.compiler = compiler;
        this.repoStore = repoStoreManager.getRepositoryStore(0);
        this.sessionStoreManager = sessionStoreManager;
        this.sessionStore = sessionStoreManager.getSessionStore(0);
        this.exec = exec;
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

    private class StoreWorkflowResult
    {
        private final StoredRevision revision;
        private final List<StoredWorkflowDefinition> workflows;

        public StoreWorkflowResult(StoredRevision revision, List<StoredWorkflowDefinition> workflows)
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

    private StoreWorkflowResult storeLocalWorkflowsImpl(
            String repositoryName, Revision revision,
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
                    return new StoreWorkflowResult(rev, storedDefs);
                });
    }

    private StoreWorkflowResult storeLocalWorkflows(
            String revisionName, ArchiveMetadata archive,
            Optional<Instant> currentTimeToSchedule)
        throws ResourceConflictException, ResourceNotFoundException
    {
        return storeLocalWorkflowsImpl(
                "default",
                Revision.builderFromArchive(revisionName, archive)
                    .archiveType("null")
                    .build(),
                archive.getWorkflowList(),
                currentTimeToSchedule);
    }

    public StoredRevision storeWorkflows(
            String revisionName,
            ArchiveMetadata archive,
            Instant currentTime)
        throws ResourceConflictException, ResourceNotFoundException
    {
        return storeLocalWorkflows(revisionName, archive, Optional.of(currentTime))
            .getRevision();
    }

    public StoredSessionAttemptWithSession storeAndStartLocalWorkflows(
            ArchiveMetadata archive,
            TaskMatchPattern taskMatchPattern,
            Config overwriteParams)
        throws ResourceConflictException, ResourceNotFoundException, SessionAttemptConflictException
    {
        StoreWorkflowResult revWfs = storeLocalWorkflows("revision", archive, Optional.absent());

        StoredRevision rev = revWfs.getRevision();
        List<StoredWorkflowDefinition> sources = revWfs.getWorkflows();

        try {
            StoredWorkflowDefinition def = taskMatchPattern.findRootWorkflow(sources);

            AttemptRequest ar = AttemptRequest.builderFromStoredWorkflow(rev, def)
                .instant(Instant.now())
                .retryAttemptName(Optional.absent())
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

    public void run()
            throws InterruptedException
    {
        exec.run();
    }

    public void runUntilAny()
            throws InterruptedException
    {
        exec.runUntilAny();
    }
}
