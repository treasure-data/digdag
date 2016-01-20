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
import io.digdag.core.schedule.ScheduleExecutor;
import io.digdag.core.workflow.TaskMatchPattern;
import io.digdag.spi.config.ConfigException;
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

    public void syncLatestRevision(StoredRevision revision, List<StoredWorkflowSource> workflowSources)
        throws ResourceConflictException
    {
        syncLatestRevision(revision, workflowSources, ImmutableList.of(), null, null);
    }

    public void syncLatestRevision(StoredRevision revision,
            List<StoredWorkflowSource> workflowSources, List<StoredScheduleSource> scheduleSources,
            SchedulerManager scheds, Date currentTime)
        throws ResourceConflictException
    {
        ImmutableList.Builder<Schedule> schedules = ImmutableList.builder();
        for (StoredScheduleSource scheduleSource : scheduleSources) {
            TaskMatchPattern taskMatchPattern = ScheduleExecutor.getScheduleWorkflowMatchPattern(scheduleSource.getConfig());

            StoredWorkflowSource workflowSource;
            try {
                workflowSource = taskMatchPattern.findRootWorkflow(workflowSources);
            }
            catch (TaskMatchPattern.NoMatchException ex) {
                throw new ConfigException(ex);
            }

            Scheduler sr = scheds.getScheduler(scheduleSource.getConfig());
            ScheduleTime firstTime = sr.getFirstScheduleTime(currentTime);
            Schedule schedule = Schedule.of(scheduleSource.getId(), workflowSource.getId(),
                    firstTime.getRunTime(), firstTime.getScheduleTime());
            schedules.add(schedule);
        }

        // TODO validate workflows and sessions
        //   * compile workflow
        //   * validate SubtaskMatchPattern

        store.syncWorkflowsToRevision(repository.getId(), workflowSources);
        store.syncSchedulesToRevision(repository.getId(), schedules.build());
    }
}
