package io.digdag.core;

import java.util.List;
import java.time.Instant;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.database.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.session.*;
import io.digdag.core.workflow.*;

public class LocalSite
{
    private static Logger logger = LoggerFactory.getLogger(LocalSite.class);

    private final WorkflowCompiler compiler;
    private final ProjectStore projectStore;
    private final SessionStore sessionStore;
    private final AttemptBuilder attemptBuilder;
    private final WorkflowExecutor exec;
    private final SchedulerManager srm;
    private final ConfigFactory cf;

    @Inject
    public LocalSite(
            WorkflowCompiler compiler,
            ProjectStoreManager projectStoreManager,
            SessionStoreManager sessionStoreManager,
            AttemptBuilder attemptBuilder,
            WorkflowExecutor exec,
            SchedulerManager srm,
            ConfigFactory cf)
    {
        this.compiler = compiler;
        this.projectStore = projectStoreManager.getProjectStore(0);
        this.sessionStore = sessionStoreManager.getSessionStore(0);
        this.attemptBuilder = attemptBuilder;
        this.exec = exec;
        this.srm = srm;
        this.cf = cf;
    }

    public AttemptBuilder getAttemptBuilder()
    {
        return attemptBuilder;
    }

    public ProjectStore getProjectStore()
    {
        return projectStore;
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
            System.out.println("getWokflowDefinitions ===============");
            System.out.println(workflowDefinitions);
            
            return workflowDefinitions;
        }
    }

    private StoreWorkflowResult storeLocalWorkflowsImpl(
            String projectName, Revision revision,
            WorkflowDefinitionList defs,
            Optional<Instant> currentTimeToSchedule)
            throws ResourceConflictException
    {
        // validate workflow
        // TODO move this to ProjectControl
        defs.get()
                .stream()
                .forEach(workflowSource -> compiler.compile(workflowSource.getName(), workflowSource.getConfig()));

        return projectStore.putAndLockProject(
                Project.of(projectName),
                (store, storedProject) -> {
                    ProjectControl lockedProj = new ProjectControl(store, storedProject);
                    StoredRevision rev = lockedProj.insertRevision(revision);
                    List<StoredWorkflowDefinition> storedDefs;
                    if (currentTimeToSchedule.isPresent()) {
                        storedDefs = lockedProj.insertWorkflowDefinitions(rev, defs.get(), srm, currentTimeToSchedule.get());
                    }
                    else {
                        storedDefs = lockedProj.insertWorkflowDefinitionsWithoutSchedules(rev, defs.get());
                    }
                    return new StoreWorkflowResult(rev, storedDefs);
                });
    }

    public StoreWorkflowResult storeLocalWorkflowsWithoutSchedule(
            String projectName,
            String revisionName,
            ArchiveMetadata archive)
            throws ResourceConflictException
    {
        return storeLocalWorkflowsImpl(
                projectName,
                Revision.builderFromArchive(revisionName, archive, cf.create())
                    .archiveType(ArchiveType.NONE)
                    .build(),
                archive.getWorkflowList(),
                Optional.absent());
    }

    public StoreWorkflowResult storeLocalWorkflows(
            String projectName,
            String revisionName,
            ArchiveMetadata archive,
            Instant currentTimeForSchedule)
            throws ResourceConflictException, ResourceNotFoundException
    {
        return storeLocalWorkflowsImpl(
                projectName,
                Revision.builderFromArchive(revisionName, archive, cf.create())
                    .archiveType(ArchiveType.NONE)
                    .build(),
                archive.getWorkflowList(),
                Optional.of(currentTimeForSchedule));
    }

    public StoredSessionAttemptWithSession submitWorkflow(
            AttemptRequest ar,
            WorkflowDefinition def)
            throws ResourceNotFoundException, SessionAttemptConflictException, AttemptLimitExceededException, TaskLimitExceededException
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
