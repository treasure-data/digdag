package io.digdag.core.repository;

import java.util.List;
import java.util.Date;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import io.digdag.spi.config.Config;
import io.digdag.core.schedule.Schedule;
import io.digdag.core.schedule.ScheduleStoreManager;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import java.util.stream.Collectors;

public class RepositoryControl
{
    private final RepositoryControlStore store;
    private final StoredRepository repository;

    public RepositoryControl(RepositoryControlStore store, StoredRepository repository)
    {
        this.store = store;
        this.repository = repository;
    }

    public StoredRepository get()
    {
        return repository;
    }

    public StoredRevision putRevision(Revision revision)
    {
        return store.putRevision(repository.getId(), revision);
    }

    public StoredWorkflowSource insertWorkflowSource(int revId, WorkflowSource source)
            throws ResourceConflictException
    {
        return store.insertWorkflowSource(revId, source);
    }

    public StoredScheduleSource insertScheduleSource(int revId, ScheduleSource schedule)
            throws ResourceConflictException
    {
        return store.insertScheduleSource(revId, schedule);
    }

    public List<StoredWorkflowSource> insertWorkflowSources(int revId, List<WorkflowSource> sources)
            throws ResourceConflictException
    {
        try {
            return sources.stream()
                .map(source -> {
                    try {
                        return insertWorkflowSource(revId, source);
                    }
                    catch (ResourceConflictException ex) {
                        throw new IllegalStateException("Database state error", ex);
                    }
                })
                .collect(Collectors.toList());
        }
        catch (IllegalStateException ex) {
            Throwables.propagateIfInstanceOf(ex.getCause(), ResourceConflictException.class);
            throw ex;
        }
    }

    public List<StoredScheduleSource> insertScheduleSources(int revId, List<ScheduleSource> sources)
            throws ResourceConflictException
    {
        try {
            return sources.stream()
                .map(source -> {
                    try {
                        return insertScheduleSource(revId, source);
                    }
                    catch (ResourceConflictException ex) {
                        throw new IllegalStateException("Database state error", ex);
                    }
                })
                .collect(Collectors.toList());
        }
        catch (IllegalStateException ex) {
            Throwables.propagateIfInstanceOf(ex.getCause(), ResourceConflictException.class);
            throw ex;
        }
    }

    public void syncSchedules(
            ScheduleStoreManager schedStoreManager, SchedulerManager scheds,
            StoredRevision revision,
            List<StoredWorkflowSource> workflows, List<StoredScheduleSource> sources,
            Date currentTime)
        throws ResourceConflictException
    {
        ImmutableList.Builder<Schedule> builder = ImmutableList.builder();
        for (StoredScheduleSource source : sources) {
            int workflowSourceId = scheds.matchWorkflow(source, workflows);
            Scheduler sr = scheds.getScheduler(source.getConfig());
            ScheduleTime firstTime = sr.getFirstScheduleTime(currentTime);
            Schedule schedule = Schedule.of(source.getId(), workflowSourceId,
                    firstTime.getRunTime(), firstTime.getScheduleTime());
            builder.add(schedule);
        }
        schedStoreManager.syncRepositorySchedules(revision.getRepositoryId(), builder.build());
    }
}
