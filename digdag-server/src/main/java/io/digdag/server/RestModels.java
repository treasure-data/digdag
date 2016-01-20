package io.digdag.server;

import io.digdag.client.api.RestRepository;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleSummary;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestWorkflow;
import io.digdag.client.api.RestTask;
import io.digdag.client.api.IdName;
import io.digdag.core.repository.StoredRepository;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredRepository;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.session.StoredSession;
import io.digdag.core.repository.StoredWorkflowSource;
import io.digdag.core.repository.StoredWorkflowSourceWithRepository;
import io.digdag.core.schedule.StoredSchedule;
import io.digdag.core.session.StoredTask;

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

    public static RestWorkflow workflow(StoredRepository repo, StoredRevision rev, StoredWorkflowSource workflow)
    {
        return workflow(repo, rev.getName(), workflow);
    }

    public static RestWorkflow workflow(StoredWorkflowSourceWithRepository wfDetails)
    {
        return workflow(wfDetails.getRepository(), wfDetails.getRevisionName(), wfDetails);
    }

    private static RestWorkflow workflow(StoredRepository repo, String revName, StoredWorkflowSource workflow)
    {
        return RestWorkflow.builder()
            .id(workflow.getId())
            .name(workflow.getName())
            .config(workflow.getConfig())
            .repository(IdName.of(repo.getId(), repo.getName()))
            .revision(revName)
            .build();
    }

    public static RestSchedule schedule(StoredSchedule sched, StoredWorkflowSource wf)
    {
        return RestSchedule.builder()
            .id(sched.getId())
            .config(sched.getConfig())
            .nextRunTime(sched.getNextRunTime().getTime() / 1000)
            .nextScheduleTime(sched.getNextScheduleTime().getTime() / 1000)
            .createdAt(sched.getCreatedAt())
            .updatedAt(sched.getCreatedAt())
            .workflow(IdName.of(wf.getId(), wf.getName()))
            .build();
    }

    public static RestScheduleSummary scheduleSummary(StoredSchedule sched)
    {
        return RestScheduleSummary.builder()
            .id(sched.getId())
            .nextRunTime(sched.getNextRunTime().getTime() / 1000)
            .nextScheduleTime(sched.getNextScheduleTime().getTime() / 1000)
            .createdAt(sched.getCreatedAt())
            .updatedAt(sched.getCreatedAt())
            .build();
    }

    public static RestSession session(StoredSession session)
    {
        return RestSession.builder()
            .id(session.getId())
            .name(session.getName())
            .params(session.getParams())
            .createdAt(session.getCreatedAt())
            .build();
    }

    public static RestTask task(StoredTask task)
    {
        return RestTask.builder()
            .id(task.getId())
            .fullName(task.getFullName())
            .parentId(task.getParentId().orNull())
            .config(task.getConfig().getNonValidated())
            .upstreams(task.getUpstreams())
            .isGroup(task.getTaskType().isGroupingOnly())
            .state(task.getState().toString().toLowerCase())
            .carryParams(task.getReport().transform(report -> report.getCarryParams()).or(task.getConfig().getLocal().getFactory().create()))
            .stateParams(task.getStateParams())
            .build();
    }
}
