package io.digdag.server;

import java.time.Instant;
import com.google.common.base.Optional;
import io.digdag.client.api.RestRepository;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleSummary;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestTask;
import io.digdag.client.api.IdName;
import io.digdag.spi.ScheduleTime;
import io.digdag.core.repository.Revision;
import io.digdag.core.repository.StoredRepository;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.session.Session;
import io.digdag.core.session.StoredTask;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;
import io.digdag.core.schedule.StoredSchedule;
import io.digdag.core.workflow.AttemptRequest;

public final class RestModels
{
    private RestModels()
    { }

    public static RestRepository repository(StoredRepository repo, StoredRevision rev)
    {
        return RestRepository.builder()
            .id(repo.getId())
            .name(repo.getName())
            .revision(rev.getName())
            .createdAt(repo.getCreatedAt())
            .updatedAt(rev.getCreatedAt())
            .archiveType(rev.getArchiveType())
            .archiveMd5(rev.getArchiveMd5())
            .build();
    }

    public static RestWorkflowDefinition workflowDefinition(StoredRepository repo, Revision rev,
            WorkflowDefinition def, Optional<ScheduleTime> nextTime)
    {
        return workflowDefinition(repo, rev.getName(), def, nextTime);
    }

    public static RestWorkflowDefinition workflowDefinition(StoredWorkflowDefinitionWithRepository wfDetails,
            Optional<ScheduleTime> nextTime)
    {
        return workflowDefinition(wfDetails.getRepository(), wfDetails.getRevisionName(), wfDetails, nextTime);
    }

    private static RestWorkflowDefinition workflowDefinition(StoredRepository repo, String revName,
            WorkflowDefinition def, Optional<ScheduleTime> nextTime)
    {
        return RestWorkflowDefinition.builder()
            .name(def.getName())
            .repository(IdName.of(repo.getId(), repo.getName()))
            .revision(revName)
            .config(def.getConfig())
            .nextScheduleTime(nextTime.transform(t -> t.getScheduleTime().getEpochSecond()))
            .nextRunTime(nextTime.transform(t -> t.getRunTime().getEpochSecond()))
            .build();
    }

    public static RestSchedule schedule(StoredSchedule sched, StoredRepository repo)
    {
        return RestSchedule.builder()
            .id(sched.getId())
            .repository(IdName.of(repo.getId(), repo.getName()))
            .workflowName(sched.getWorkflowName())
            .nextRunTime(sched.getNextRunTime().getEpochSecond())
            .nextScheduleTime(sched.getNextScheduleTime().getEpochSecond())
            .build();
    }

    public static RestScheduleSummary scheduleSummary(StoredSchedule sched)
    {
        return RestScheduleSummary.builder()
            .id(sched.getId())
            .workflowName(sched.getWorkflowName())
            .nextRunTime(sched.getNextRunTime().getEpochSecond())
            .nextScheduleTime(sched.getNextScheduleTime().getEpochSecond())
            .createdAt(sched.getCreatedAt())
            .updatedAt(sched.getCreatedAt())
            .build();
    }

    public static RestSession session(StoredSessionAttemptWithSession attempt, String repositoryName)
    {
        return session(attempt, attempt.getSession(), repositoryName);
    }

    public static RestSession session(StoredSessionAttempt attempt, Session session, String repositoryName)
    {
        return session(attempt, session.getRepositoryId(), repositoryName, session.getWorkflowName(), session.getInstant());
    }

    public static RestSession session(StoredSessionAttempt attempt, AttemptRequest ar, String repositoryName)
    {
        return session(attempt, ar.getStored().getRepositoryId(), repositoryName, ar.getWorkflowName(), ar.getInstant());
    }

    private static RestSession session(StoredSessionAttempt attempt,
            int repositoryId, String repositoryName, String workflowName, Instant sessionTime)
    {
        return RestSession.builder()
            .id(attempt.getId())
            .repository(IdName.of(repositoryId, repositoryName))
            .workflowName(workflowName)
            .sessionTime(sessionTime.getEpochSecond())
            .retryAttemptName(attempt.getRetryAttemptName())
            .done(attempt.getStateFlags().isDone())
            .success(attempt.getStateFlags().isSuccess())
            .cancelRequested(attempt.getStateFlags().isCancelRequested())
            .params(attempt.getParams())
            .createdAt(attempt.getCreatedAt())
            .build();
    }

    public static RestTask task(StoredTask task)
    {
        return RestTask.builder()
            .id(task.getId())
            .fullName(task.getFullName())
            .parentId(task.getParentId())
            .config(task.getConfig().getNonValidated())
            .upstreams(task.getUpstreams())
            .isGroup(task.getTaskType().isGroupingOnly())
            .state(task.getState().toString().toLowerCase())
            .carryParams(task.getReport().transform(report -> report.getCarryParams()).or(task.getConfig().getLocal().getFactory().create()))
            .stateParams(task.getStateParams())
            .updatedAt(task.getUpdatedAt())
            .retryAt(task.getRetryAt())
            .build();
    }
}
