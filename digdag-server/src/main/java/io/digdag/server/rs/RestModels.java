package io.digdag.server.rs;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import com.google.common.base.Optional;
import io.digdag.client.api.RestRepository;
import io.digdag.client.api.RestRevision;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleSummary;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestWorkflowSessionTime;
import io.digdag.client.api.RestTask;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.api.RestDirectDownloadHandle;
import io.digdag.client.api.RestDirectUploadHandle;
import io.digdag.client.api.IdName;
import io.digdag.client.api.NameOptionalId;
import io.digdag.client.api.NameLongId;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.LogFileHandle;
import io.digdag.spi.DirectDownloadHandle;
import io.digdag.spi.DirectUploadHandle;
import io.digdag.core.repository.Revision;
import io.digdag.core.repository.StoredRepository;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;
import io.digdag.core.session.Session;
import io.digdag.core.session.StoredTask;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.schedule.StoredSchedule;
import io.digdag.core.workflow.AttemptRequest;

public final class RestModels
{
    private RestModels()
    { }

    public static RestRepository repository(StoredRepository repo, StoredRevision lastRevision)
    {
        return RestRepository.builder()
            .id(repo.getId())
            .name(repo.getName())
            .revision(lastRevision.getName())
            .createdAt(repo.getCreatedAt())
            .updatedAt(lastRevision.getCreatedAt())
            .archiveType(lastRevision.getArchiveType())
            .archiveMd5(lastRevision.getArchiveMd5())
            .build();
    }

    public static RestRevision revision(StoredRepository repo, StoredRevision rev)
    {
        return RestRevision.builder()
            .revision(rev.getName())
            .createdAt(rev.getCreatedAt())
            .archiveType(rev.getArchiveType())
            .archiveMd5(rev.getArchiveMd5())
            .build();
    }

    public static RestWorkflowDefinition workflowDefinition(StoredRepository repo, Revision rev,
            StoredWorkflowDefinition def)
    {
        return workflowDefinition(repo, rev.getName(), def);
    }

    public static RestWorkflowDefinition workflowDefinition(StoredWorkflowDefinitionWithRepository wfDetails)
    {
        return workflowDefinition(wfDetails.getRepository(), wfDetails.getRevisionName(), wfDetails);
    }

    private static RestWorkflowDefinition workflowDefinition(
            StoredRepository repo, String revName,
            StoredWorkflowDefinition def)
    {
        return RestWorkflowDefinition.builder()
            .id(def.getId())
            .name(def.getName())
            .repository(IdName.of(repo.getId(), repo.getName()))
            .revision(revName)
            .config(def.getConfig())
            .build();
    }

    public static RestWorkflowSessionTime workflowSessionTime(
            StoredWorkflowDefinitionWithRepository def,
            Instant sessionTime, ZoneId timeZone)
    {
        StoredRepository repo = def.getRepository();
        return RestWorkflowSessionTime.builder()
            .repository(IdName.of(repo.getId(), repo.getName()))
            .revision(def.getRevisionName())
            .sessionTime(OffsetDateTime.ofInstant(sessionTime, timeZone))
            .timeZone(timeZone)
            .build();
    }

    public static RestSchedule schedule(StoredSchedule sched, StoredRepository repo, ZoneId timeZone)
    {
        return RestSchedule.builder()
            .id(sched.getId())
            .repository(IdName.of(repo.getId(), repo.getName()))
            .workflow(NameLongId.of(sched.getWorkflowName(), sched.getWorkflowDefinitionId()))
            .nextRunTime(sched.getNextRunTime())
            .nextScheduleTime(OffsetDateTime.ofInstant(sched.getNextScheduleTime(), timeZone))
            .build();
    }

    public static RestScheduleSummary scheduleSummary(StoredSchedule sched, ZoneId timeZone)
    {
        return RestScheduleSummary.builder()
            .id(sched.getId())
            .workflow(NameLongId.of(sched.getWorkflowName(), sched.getWorkflowDefinitionId()))
            .nextRunTime(sched.getNextRunTime())
            .nextScheduleTime(OffsetDateTime.ofInstant(sched.getNextScheduleTime(), timeZone))
            .createdAt(sched.getCreatedAt())
            .updatedAt(sched.getCreatedAt())
            .build();
    }

    public static RestSessionAttempt attempt(StoredSessionAttemptWithSession attempt, String repositoryName)
    {
        return RestSessionAttempt.builder()
            .id(attempt.getId())
            .repository(IdName.of(attempt.getSession().getRepositoryId(), repositoryName))
            .workflow(NameOptionalId.of(attempt.getSession().getWorkflowName(), attempt.getWorkflowDefinitionId()))
            .sessionUuid(attempt.getSessionUuid())
            .sessionTime(OffsetDateTime.ofInstant(attempt.getSession().getSessionTime(), attempt.getTimeZone()))
            .retryAttemptName(attempt.getRetryAttemptName())
            .done(attempt.getStateFlags().isDone())
            .success(attempt.getStateFlags().isSuccess())
            .cancelRequested(attempt.getStateFlags().isCancelRequested())
            .params(attempt.getParams())
            .createdAt(attempt.getCreatedAt())
            .build();
    }

    public static RestTask task(ArchivedTask task)
    {
        return RestTask.builder()
            .id(task.getId())
            .fullName(task.getFullName())
            .parentId(task.getParentId())
            .config(task.getConfig().getNonValidated())
            .upstreams(task.getUpstreams())
            .isGroup(task.getTaskType().isGroupingOnly())
            .state(task.getState().toString().toLowerCase())
            .exportParams(task.getConfig().getExport().deepCopy().merge(task.getExportParams()))
            .storeParams(task.getStoreParams())
            .stateParams(task.getStateParams())
            .updatedAt(task.getUpdatedAt())
            .retryAt(task.getRetryAt())
            .build();
    }

    public static RestLogFileHandle logFileHandle(LogFileHandle handle)
    {
        return RestLogFileHandle.builder()
            .fileName(handle.getFileName())
            .fileSize(handle.getFileSize())
            .taskName(handle.getTaskName())
            .fileTime(handle.getFirstLogTime())
            .agentId(handle.getAgentId())
            .direct(handle.getDirect().transform(it -> directDownloadHandle(it)))
            .build();
    }

    public static RestDirectDownloadHandle directDownloadHandle(DirectDownloadHandle handle)
    {
        return RestDirectDownloadHandle.builder()
            .type(handle.getType())
            .url(handle.getUrl())
            .build();
    }

    public static RestDirectUploadHandle directUploadHandle(DirectUploadHandle handle)
    {
        return RestDirectUploadHandle.builder()
            .type(handle.getType())
            .url(handle.getUrl())
            .build();
    }
}
