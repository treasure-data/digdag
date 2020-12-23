package io.digdag.core.workflow;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.core.Limits;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.session.Session;
import io.digdag.core.session.SessionAttempt;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionTransaction;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import java.time.Instant;

import static java.util.Locale.ENGLISH;

public class WorkflowSubmitter
{
    private final int siteId;
    private final SessionTransaction transaction;
    private final ProjectStore projectStore;
    private final SessionStore sessionStore;
    private final TransactionManager transactionManager;
    private final Limits limits;


    WorkflowSubmitter(int siteId, SessionTransaction transaction,
            ProjectStore projectStore, SessionStore sessionStore, TransactionManager transactionManager, Limits limits)
    {
        this.siteId = siteId;
        this.transaction = transaction;
        this.projectStore = projectStore;
        this.sessionStore = sessionStore;
        this.transactionManager = transactionManager;
        this.limits = limits;
    }

    public StoredSessionAttemptWithSession submitDelayedAttempt(
            AttemptRequest ar,
            Optional<Long> dependentSessionId)
        throws ResourceNotFoundException, AttemptLimitExceededException, SessionAttemptConflictException
    {
        int projId = ar.getStored().getProjectId();
        Session session = Session.of(projId, ar.getWorkflowName(), ar.getSessionTime());

        SessionAttempt attempt = SessionAttempt.of(
                ar.getRetryAttemptName(),
                ar.getSessionParams(),
                ar.getTimeZone(),
                Optional.of(ar.getStored().getWorkflowDefinitionId()));

        TaskConfig.validateAttempt(attempt);

        try {
            long activeAttempts = transaction.getActiveAttemptCount();

            if (activeAttempts + 1 > limits.maxAttempts()) {
                throw new AttemptLimitExceededException("Too many attempts running. Limit: " + limits.maxAttempts() + ", Current: " + activeAttempts);
            }

            return transaction.putAndLockSession(session, (store, storedSession) -> {
                StoredProject proj = projectStore.getProjectById(projId);
                if (proj.getDeletedAt().isPresent()) {
                    throw new ResourceNotFoundException(String.format(ENGLISH,
                                "Project id={} name={} is already deleted",
                                proj.getId(), proj.getName()));
                }
                StoredSessionAttempt storedAttempt = store.insertDelayedAttempt(storedSession.getId(), projId, attempt, dependentSessionId);  // this may throw ResourceConflictException
                return StoredSessionAttemptWithSession.of(siteId, storedSession, storedAttempt);
            });
        }
        catch (ResourceConflictException sessionAlreadyExists) {
            transactionManager.reset();
            StoredSessionAttemptWithSession conflicted;
            if (ar.getRetryAttemptName().isPresent()) {
                conflicted = sessionStore
                    .getAttemptByName(session.getProjectId(), session.getWorkflowName(), session.getSessionTime(), ar.getRetryAttemptName().get());
            }
            else {
                conflicted = sessionStore
                    .getLastAttemptByName(session.getProjectId(), session.getWorkflowName(), session.getSessionTime());
            }
            throw new SessionAttemptConflictException("Session already exists", sessionAlreadyExists, conflicted);
        }
    }

    public Optional<Instant> getLastExecutedSessionTime(
            int projectId, String workflowName,
            Instant beforeThisSessionTime)
    {
        return transaction.getLastExecutedSessionTime(
                projectId, workflowName,
                beforeThisSessionTime);
    }
}
