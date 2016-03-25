package io.digdag.core;

import java.util.List;
import java.time.Instant;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.session.*;
import io.digdag.core.workflow.*;

public class LocalSite
{
    private static Logger logger = LoggerFactory.getLogger(LocalSite.class);

    private final WorkflowCompiler compiler;
    private final RepositoryStore repositoryStore;
    private final SessionStore sessionStore;
    private final AttemptBuilder attemptBuilder;
    private final WorkflowExecutor exec;
    private final SchedulerManager srm;

    @Inject
    public LocalSite(
            WorkflowCompiler compiler,
            RepositoryStoreManager repoStoreManager,
            SessionStoreManager sessionStoreManager,
            AttemptBuilder attemptBuilder,
            WorkflowExecutor exec,
            SchedulerManager srm)
    {
        this.compiler = compiler;
        this.repositoryStore = repoStoreManager.getRepositoryStore(0);
        this.sessionStore = sessionStoreManager.getSessionStore(0);
        this.attemptBuilder = attemptBuilder;
        this.exec = exec;
        this.srm = srm;
    }

    public AttemptBuilder getAttemptBuilder()
    {
        return attemptBuilder;
    }

    public RepositoryStore getRepositoryStore()
    {
        return repositoryStore;
    }

    public SessionStore getSessionStore()
    {
        return sessionStore;
    }

    public static class StoreWorkflowResult
    {
        private final StoredRevision revision;
        private final List<StoredWorkflowDefinition> workflowDefinitions;

        public StoreWorkflowResult(StoredRevision revision, List<StoredWorkflowDefinition> workflowDefinitions)
        {
            this.revision = revision;
            this.workflowDefinitions = workflowDefinitions;
        }

        public StoredRevision getRevision()
        {
            return revision;
        }

        public List<StoredWorkflowDefinition> getWorkflowDefinitions()
        {
            return workflowDefinitions;
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

        return repositoryStore.putAndLockRepository(
                Repository.of(repositoryName),
                (store, storedRepo) -> {
                    RepositoryControl lockedRepo = new RepositoryControl(store, storedRepo);
                    StoredRevision rev = lockedRepo.insertRevision(revision);
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

    public StoreWorkflowResult storeLocalWorkflowsWithoutSchedule(
            String repositoryName,
            String revisionName,
            ArchiveMetadata archive)
        throws ResourceConflictException, ResourceNotFoundException
    {
        return storeLocalWorkflowsImpl(
                repositoryName,
                Revision.builderFromArchive(revisionName, archive)
                    .archiveType("none")
                    .build(),
                archive.getWorkflowList(),
                Optional.absent());
    }

    public StoreWorkflowResult storeLocalWorkflows(
            String repositoryName,
            String revisionName,
            ArchiveMetadata archive,
            Instant currentTimeForSchedule)
        throws ResourceConflictException, ResourceNotFoundException
    {
        return storeLocalWorkflowsImpl(
                repositoryName,
                Revision.builderFromArchive(revisionName, archive)
                    .archiveType("none")
                    .build(),
                archive.getWorkflowList(),
                Optional.of(currentTimeForSchedule));
    }

    public StoredSessionAttemptWithSession submitWorkflow(
            AttemptRequest ar,
            WorkflowDefinition def)
        throws ResourceNotFoundException, SessionAttemptConflictException
    {
        return exec.submitWorkflow(0, ar, def);
    }

    public void run()
        throws InterruptedException
    {
        exec.run();
    }

    public StoredSessionAttemptWithSession runUntilDone(long attemptId)
        throws ResourceNotFoundException, InterruptedException
    {
        return exec.runUntilDone(attemptId);
    }

    public void runUntilAllDone()
        throws InterruptedException
    {
        exec.runUntilAllDone();
    }

    public boolean killAttempt(long attemptId)
        throws ResourceNotFoundException
    {
        return exec.killAttemptById(0, attemptId);
    }
}
