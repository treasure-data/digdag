package io.digdag.server.rs;

import io.digdag.client.api.IdName;
import io.digdag.client.api.NameLongId;
import io.digdag.client.api.NameOptionalId;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestRevision;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleSummary;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestTask;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestWorkflowSessionTime;
import io.digdag.client.api.RestDirectDownloadHandle;
import io.digdag.core.repository.ProjectMap;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.Revision;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.core.schedule.StoredSchedule;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.Session;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.StoredSessionWithLastAttempt;
import io.digdag.spi.LogFileHandle;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public final class RestModels
{
    private RestModels()
    { }

    public static RestProject project(StoredProject proj, StoredRevision lastRevision)
    {
        return RestProject.builder()
            .id(proj.getId())
            .name(proj.getName())
            .revision(lastRevision.getName())
            .createdAt(proj.getCreatedAt())
            .updatedAt(lastRevision.getCreatedAt())
            .deletedAt(proj.getDeletedAt())
            .archiveType(lastRevision.getArchiveType().getName())
            .archiveMd5(lastRevision.getArchiveMd5())
            .build();
    }

    public static RestRevision revision(StoredProject proj, StoredRevision rev)
    {
        return RestRevision.builder()
            .revision(rev.getName())
            .createdAt(rev.getCreatedAt())
            .archiveType(rev.getArchiveType().getName())
            .archiveMd5(rev.getArchiveMd5())
            .build();
    }

    public static RestWorkflowDefinition workflowDefinition(StoredProject proj, Revision rev,
            StoredWorkflowDefinition def)
    {
        return workflowDefinition(proj, rev.getName(), def);
    }

    public static RestWorkflowDefinition workflowDefinition(StoredWorkflowDefinitionWithProject wfDetails)
    {
        return workflowDefinition(wfDetails.getProject(), wfDetails.getRevisionName(), wfDetails);
    }

    private static RestWorkflowDefinition workflowDefinition(
            StoredProject proj, String revName,
            StoredWorkflowDefinition def)
    {
        return RestWorkflowDefinition.builder()
            .id(def.getId())
            .name(def.getName())
            .project(IdName.of(proj.getId(), proj.getName()))
            .revision(revName)
            .config(def.getConfig())
            .build();
    }

    public static RestWorkflowSessionTime workflowSessionTime(
            StoredWorkflowDefinitionWithProject def,
            Instant sessionTime, ZoneId timeZone)
    {
        StoredProject proj = def.getProject();
        return RestWorkflowSessionTime.builder()
            .project(IdName.of(proj.getId(), proj.getName()))
            .revision(def.getRevisionName())
            .sessionTime(OffsetDateTime.ofInstant(sessionTime, timeZone))
            .timeZone(timeZone)
            .build();
    }

    public static RestSchedule schedule(StoredSchedule sched, StoredProject proj, ZoneId timeZone)
    {
        return RestSchedule.builder()
            .id(sched.getId())
            .project(IdName.of(proj.getId(), proj.getName()))
            .workflow(NameLongId.of(sched.getWorkflowName(), sched.getWorkflowDefinitionId()))
            .nextRunTime(sched.getNextRunTime())
            .nextScheduleTime(OffsetDateTime.ofInstant(sched.getNextScheduleTime(), timeZone))
            .disabledAt(sched.getDisabledAt())
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
            .disabledAt(sched.getDisabledAt())
            .build();
    }


    public static RestSession session(StoredSessionWithLastAttempt session, String projectName)
    {
        StoredSessionAttempt attempt = session.getLastAttempt();
        return RestSession.builder()
                .id(session.getId())
                .project(IdName.of(session.getProjectId(), projectName))
                .workflow(NameOptionalId.of(session.getWorkflowName(), attempt.getWorkflowDefinitionId()))
                .sessionUuid(session.getUuid())
                .sessionTime(OffsetDateTime.ofInstant(session.getSessionTime(), attempt.getTimeZone()))
                .lastAttempt(RestSession.Attempt.builder()
                        .id(attempt.getId())
                        .retryAttemptName(attempt.getRetryAttemptName())
                        .done(attempt.getStateFlags().isDone())
                        .success(attempt.getStateFlags().isSuccess())
                        .cancelRequested(attempt.getStateFlags().isCancelRequested())
                        .params(attempt.getParams())
                        .createdAt(attempt.getCreatedAt())
                        .finishedAt(attempt.getFinishedAt())
                        .build())
                .build();
    }

    public static RestSessionAttempt attempt(StoredSessionAttemptWithSession attempt, String projectName)
    {
        return attempt(attempt.getSessionUuid(), attempt.getSession(), attempt, projectName);
    }

    public static RestSessionAttempt attempt(StoredSession session, StoredSessionAttempt attempt, String projectName)
    {
        return attempt(session.getUuid(), session, attempt, projectName);
    }

    public static RestSessionAttempt attempt(UUID sessionUuid, Session session, StoredSessionAttempt attempt, String projectName)
    {
        return RestSessionAttempt.builder()
            .id(attempt.getId())
            .project(IdName.of(session.getProjectId(), projectName))
            .workflow(NameOptionalId.of(session.getWorkflowName(), attempt.getWorkflowDefinitionId()))
            .sessionId(attempt.getSessionId())
            .sessionUuid(sessionUuid)
            .sessionTime(OffsetDateTime.ofInstant(session.getSessionTime(), attempt.getTimeZone()))
            .retryAttemptName(attempt.getRetryAttemptName())
            .done(attempt.getStateFlags().isDone())
            .success(attempt.getStateFlags().isSuccess())
            .cancelRequested(attempt.getStateFlags().isCancelRequested())
            .params(attempt.getParams())
            .createdAt(attempt.getCreatedAt())
            .finishedAt(attempt.getFinishedAt())
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
            .startedAt(task.getStartedAt())
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
            .direct(handle.getDirect().transform(it -> RestDirectDownloadHandle.of(it.getUrl().toString())))
            .build();
    }

    static List<RestSession> sessionModels(
            ProjectStore ps,
            List<StoredSessionWithLastAttempt> sessions)
    {
        ProjectMap projs = ps.getProjectsByIdList(
                sessions.stream()
                        .map(Session::getProjectId)
                        .collect(Collectors.toList()));

        return sessions.stream()
                .map(session -> {
                    try {
                        return session(session, projs.get(session.getProjectId()).getName());
                    }
                    catch (ResourceNotFoundException ex) {
                        // must not happen
                        return null;
                    }
                })
                .filter(a -> a != null)
                .collect(Collectors.toList());
    }

    static List<RestSessionAttempt> attemptModels(
            ProjectStoreManager rm, int siteId,
            List<StoredSessionAttemptWithSession> attempts)
    {
        ProjectMap projs = rm.getProjectStore(siteId)
            .getProjectsByIdList(
                    attempts.stream()
                    .map(attempt -> attempt.getSession().getProjectId())
                    .collect(Collectors.toList()));

        return attempts.stream()
            .map(attempt -> {
                try {
                    return attempt(attempt, projs.get(attempt.getSession().getProjectId()).getName());
                }
                catch (ResourceNotFoundException ex) {
                    // must not happen
                    return null;
                }
            })
            .filter(a -> a != null)
            .collect(Collectors.toList());
    }
}
