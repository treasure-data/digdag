package io.digdag.server.rs;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.api.Id;
import io.digdag.client.api.IdAndName;
import io.digdag.client.api.NameOptionalId;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.client.api.RestLogFileHandleCollection;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestProjectCollection;
import io.digdag.client.api.RestRevision;
import io.digdag.client.api.RestRevisionCollection;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleAttemptCollection;
import io.digdag.client.api.RestScheduleCollection;
import io.digdag.client.api.RestScheduleSummary;
import io.digdag.client.api.RestSecret;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionCollection;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestSessionAttemptCollection;
import io.digdag.client.api.RestTask;
import io.digdag.client.api.RestTaskCollection;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestWorkflowDefinitionCollection;
import io.digdag.client.api.RestWorkflowSessionTime;
import io.digdag.client.api.RestDirectDownloadHandle;
import io.digdag.core.repository.ProjectMap;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.Revision;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.core.repository.TimeZoneMap;
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

import static java.util.Locale.ENGLISH;

public final class RestModels
{
    private RestModels()
    { }

    public static RestProject project(StoredProject proj, StoredRevision lastRevision)
    {
        return RestProject.builder()
            .id(id(proj.getId()))
            .name(proj.getName())
            .revision(lastRevision.getName())
            .createdAt(proj.getCreatedAt())
            .updatedAt(lastRevision.getCreatedAt())
            .deletedAt(proj.getDeletedAt())
            .archiveType(lastRevision.getArchiveType().getName())
            .archiveMd5(lastRevision.getArchiveMd5())
            .build();
    }

    public static RestProjectCollection projectCollection(List<RestProject> collection)
    {
        return RestProjectCollection.builder()
            .projects(collection)
            .build();
    }

    public static RestRevision revision(StoredProject proj, StoredRevision rev)
    {
        return RestRevision.builder()
            .revision(rev.getName())
            .createdAt(rev.getCreatedAt())
            .archiveType(rev.getArchiveType().getName())
            .archiveMd5(rev.getArchiveMd5())
            .userInfo(Optional.of(rev.getUserInfo()))
            .build();
    }

    public static RestRevisionCollection revisionCollection(StoredProject proj, List<StoredRevision> revs)
    {
        List<RestRevision> collection = revs.stream()
            .map(rev -> RestModels.revision(proj, rev))
            .collect(Collectors.toList());
        return RestRevisionCollection.builder()
            .revisions(collection)
            .build();
    }

    public static RestSecret secret(StoredProject proj, String key)
    {
        return RestSecret.builder()
                .key(key)
                .project(IdAndName.of(RestModels.id(proj.getId()), proj.getName()))
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
            .id(id(def.getId()))
            .name(def.getName())
            .project(IdAndName.of(id(proj.getId()), proj.getName()))
            .revision(revName)
            .timeZone(def.getTimeZone())
            .config(def.getConfig())
            .build();
    }

    public static RestWorkflowDefinitionCollection workflowDefinitionCollection(
            List<StoredWorkflowDefinitionWithProject> defs)
    {
        List<RestWorkflowDefinition> collection = defs.stream()
            .map(def -> RestModels.workflowDefinition(def))
            .collect(Collectors.toList());
        return RestWorkflowDefinitionCollection.builder()
            .workflows(collection)
            .build();
    }

    public static RestWorkflowDefinitionCollection workflowDefinitionCollection(
            StoredProject proj, StoredRevision rev, List<StoredWorkflowDefinition> defs)
    {
        List<RestWorkflowDefinition> collection = defs.stream()
                .map(def -> RestModels.workflowDefinition(proj, rev, def))
                .collect(Collectors.toList());
        return RestWorkflowDefinitionCollection.builder()
            .workflows(collection)
            .build();
    }

    public static RestWorkflowSessionTime workflowSessionTime(
            StoredWorkflowDefinitionWithProject def,
            Instant sessionTime, ZoneId timeZone)
    {
        StoredProject proj = def.getProject();
        return RestWorkflowSessionTime.builder()
            .project(IdAndName.of(id(proj.getId()), proj.getName()))
            .revision(def.getRevisionName())
            .sessionTime(OffsetDateTime.ofInstant(sessionTime, timeZone))
            .timeZone(timeZone)
            .build();
    }

    public static RestSchedule schedule(StoredSchedule sched, StoredProject proj, ZoneId timeZone)
    {
        return RestSchedule.builder()
            .id(id(sched.getId()))
            .project(IdAndName.of(id(proj.getId()), proj.getName()))
            .workflow(IdAndName.of(id(sched.getWorkflowDefinitionId()), sched.getWorkflowName()))
            .nextRunTime(sched.getNextRunTime())
            .nextScheduleTime(OffsetDateTime.ofInstant(sched.getNextScheduleTime(), timeZone))
            .disabledAt(sched.getDisabledAt())
            .build();
    }

    static RestScheduleCollection scheduleCollection(
            ProjectStore projectStore, List<StoredSchedule> scheds)
    {
        if (scheds.isEmpty()) {
            return RestScheduleCollection.builder()
                .schedules(ImmutableList.of())
                .build();
        }

        ProjectMap projs = projectStore.getProjectsByIdList(
                scheds.stream()
                .map(StoredSchedule::getProjectId)
                .collect(Collectors.toList()));
        TimeZoneMap defTimeZones = projectStore.getWorkflowTimeZonesByIdList(
                scheds.stream()
                .map(StoredSchedule::getWorkflowDefinitionId)
                .collect(Collectors.toList()));

        List<RestSchedule> collection = scheds.stream()
            .map(sched -> {
                try {
                    return RestModels.schedule(sched,
                            projs.get(sched.getProjectId()),
                            defTimeZones.get(sched.getWorkflowDefinitionId()));
                }
                catch (ResourceNotFoundException ex) {
                    return null;
                }
            })
            .filter(sched -> sched != null)
            .collect(Collectors.toList());

        return RestScheduleCollection.builder()
            .schedules(collection)
            .build();
    }


    public static RestScheduleSummary scheduleSummary(StoredSchedule sched, StoredProject proj, ZoneId timeZone)
    {
        return RestScheduleSummary.builder()
            .id(id(sched.getId()))
            .workflow(IdAndName.of(id(sched.getWorkflowDefinitionId()), sched.getWorkflowName()))
            .project(IdAndName.of(id(proj.getId()), proj.getName()))
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
                .id(id(session.getId()))
                .project(IdAndName.of(id(session.getProjectId()), projectName))
                .workflow(NameOptionalId.of(session.getWorkflowName(), attempt.getWorkflowDefinitionId().transform(w -> id(w))))
                .sessionUuid(session.getUuid())
                .sessionTime(OffsetDateTime.ofInstant(session.getSessionTime(), attempt.getTimeZone()))
                .lastAttempt(RestSession.Attempt.builder()
                        .id(id(attempt.getId()))
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

    static RestSessionCollection sessionCollection(
            ProjectStore ps, List<StoredSessionWithLastAttempt> sessions)
    {
        ProjectMap projs = ps.getProjectsByIdList(
                sessions.stream()
                        .map(Session::getProjectId)
                        .collect(Collectors.toList()));

        List<RestSession> collection = sessions.stream()
                .map(session -> {
                    try {
                        return session(session, projs.get(session.getProjectId()).getName());
                    }
                    catch (ResourceNotFoundException ex) {
                        throw new IllegalStateException(String.format(ENGLISH,
                                    "An session id=%d references a nonexistent project id=%d",
                                    session.getId(), session.getProjectId()));
                    }
                })
                .collect(Collectors.toList());

        return RestSessionCollection.builder()
            .sessions(collection)
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
            .id(id(attempt.getId()))
            .index(attempt.getIndex())
            .project(IdAndName.of(id(session.getProjectId()), projectName))
            .workflow(NameOptionalId.of(session.getWorkflowName(), attempt.getWorkflowDefinitionId().transform(w -> id(w))))
            .sessionId(id(attempt.getSessionId()))
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
            .id(id(task.getId()))
            .fullName(task.getFullName())
            .parentId(task.getParentId().transform(p -> id(p)))
            .config(task.getConfig().getNonValidated())
            .upstreams(task.getUpstreams().stream()
                    .map(u -> id(u))
                    .collect(Collectors.toList()))
            .isGroup(task.getTaskType().isGroupingOnly())
            .state(task.getState().toString().toLowerCase())
            .cancelRequested(task.getStateFlags().isCancelRequested())
            .exportParams(task.getConfig().getExport().deepCopy().merge(task.getExportParams()))
            .storeParams(task.getStoreParams())
            .stateParams(task.getStateParams())
            .updatedAt(task.getUpdatedAt())
            .retryAt(task.getRetryAt())
            .startedAt(task.getStartedAt())
            .error(task.getError())
            .build();
    }

    public static RestTaskCollection taskCollection(List<ArchivedTask> tasks)
    {
        List<RestTask> collection = tasks.stream()
            .map(task -> RestModels.task(task))
            .collect(Collectors.toList());
        return RestTaskCollection.builder()
            .tasks(collection)
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

    public static RestLogFileHandleCollection logFileHandleCollection(List<LogFileHandle> handles)
    {
        List<RestLogFileHandle> collection = handles.stream()
            .map(it -> RestModels.logFileHandle(it))
            .collect(Collectors.toList());
        return RestLogFileHandleCollection.builder()
            .files(collection)
            .build();
    }

    static RestSessionAttemptCollection attemptCollection(
            ProjectStore ps, List<StoredSessionAttemptWithSession> attempts)
    {
        ProjectMap projs = ps
            .getProjectsByIdList(
                    attempts.stream()
                    .map(attempt -> attempt.getSession().getProjectId())
                    .collect(Collectors.toList()));

        List<RestSessionAttempt> collection = attempts.stream()
            .map(attempt -> {
                try {
                    return attempt(attempt, projs.get(attempt.getSession().getProjectId()).getName());
                }
                catch (ResourceNotFoundException ex) {
                    throw new IllegalStateException(String.format(ENGLISH,
                                "An attempt id=%d references a nonexistent project id=%d",
                                attempt.getId(), attempt.getSession().getProjectId()));
                }
            })
            .collect(Collectors.toList());

        return attemptCollection(collection);
    }

    static RestSessionAttemptCollection attemptCollection(
            List<RestSessionAttempt> collection)
    {
        return RestSessionAttemptCollection.builder()
            .attempts(collection)
            .build();
    }

    static RestScheduleAttemptCollection attemptCollection(
            StoredSchedule sched, StoredProject prj, ProjectStore ps, List<StoredSessionAttemptWithSession> attempts)
    {
        ProjectMap projs = ps
                .getProjectsByIdList(
                        attempts.stream()
                                .map(attempt -> attempt.getSession().getProjectId())
                                .collect(Collectors.toList()));

        List<RestSessionAttempt> collection = attempts.stream()
                .map(attempt -> {
                    try {
                        return attempt(attempt, projs.get(attempt.getSession().getProjectId()).getName());
                    }
                    catch (ResourceNotFoundException ex) {
                        throw new IllegalStateException(String.format(ENGLISH,
                                "An attempt id=%d references a nonexistent project id=%d",
                                attempt.getId(), attempt.getSession().getProjectId()));
                    }
                })
                .collect(Collectors.toList());
        return attemptCollection(sched, prj, collection);
    }

    static RestScheduleAttemptCollection attemptCollection(
            StoredSchedule sched, StoredProject prj, List<RestSessionAttempt> collection)
    {
        return RestScheduleAttemptCollection.builder()
                .id(id(sched.getId()))
                .workflow(IdAndName.of(id(sched.getWorkflowDefinitionId()), sched.getWorkflowName()))
                .project(IdAndName.of(id(prj.getId()),prj.getName()))
                .attempts(collection)
                .build();
    }


    static Id id(int id)
    {
        return Id.of(Long.toString(id));
    }

    static Id id(long id)
    {
        return Id.of(Long.toString(id));
    }

    static int parseProjectId(Id id)
    {
        return Integer.parseInt(id.get());
    }

    static long parseWorkflowId(Id id)
    {
        return Long.parseLong(id.get());
    }

    static long parseAttemptId(Id id)
    {
        return Long.parseLong(id.get());
    }
}
