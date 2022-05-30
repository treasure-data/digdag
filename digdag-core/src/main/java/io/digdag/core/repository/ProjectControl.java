package io.digdag.core.repository;

import java.util.Collections;
import java.util.List;
import java.time.Instant;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Optional;
import io.digdag.commons.ThrowablesUtil;
import io.digdag.core.repository.ProjectControlStore.ScheduleTimeWithInfo;
import io.digdag.core.schedule.Schedule;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

public class ProjectControl
{
    private static final Logger logger = LoggerFactory.getLogger(ProjectControl.class);

    public static interface DeleteProjectAction <T>
    {
        public T call(ProjectControl control, StoredProject project)
            throws ResourceNotFoundException;
    }

    public static <T> T deleteProject(ProjectStore rs, int projId, DeleteProjectAction<T> callback)
        throws ResourceNotFoundException
    {
        return rs.deleteProject(projId, (store, proj) -> {
            ProjectControl control = new ProjectControl(store, proj);
            T res = callback.call(control, proj);
            control.deleteSchedules();
            return res;
        });
    }

    private final ProjectControlStore store;
    private final StoredProject project;

    public ProjectControl(ProjectControlStore store, StoredProject project)
    {
        this.store = store;
        this.project = project;
    }

    public StoredProject get()
    {
        return project;
    }

    public StoredRevision insertRevision(Revision revision)
        throws ResourceConflictException
    {
        return store.insertRevision(project.getId(), revision);
    }

    public void insertRevisionArchiveData(int revId, byte[] data)
        throws ResourceConflictException
    {
        store.insertRevisionArchiveData(revId, data);
    }

    public List<StoredWorkflowDefinition> insertWorkflowDefinitions(
            StoredRevision revision, List<WorkflowDefinition> defs,
            SchedulerManager srm, Instant currentTime)
            throws ResourceConflictException
    {
        return insertWorkflowDefinitions(revision, defs, Collections.emptyMap(), srm, currentTime);
    }

    public List<StoredWorkflowDefinition> insertWorkflowDefinitions(
            StoredRevision revision, List<WorkflowDefinition> defs, Map<String,Boolean> clearSchedules,
            SchedulerManager srm, Instant currentTime)
        throws ResourceConflictException
    {
        List<StoredWorkflowDefinition> list = insertWorkflowDefinitionsWithoutSchedules(revision, defs);
        updateSchedules(revision, list, clearSchedules, srm, currentTime);
        return list;
    }

    public List<StoredWorkflowDefinition> insertWorkflowDefinitionsWithoutSchedules(
            StoredRevision revision, List<WorkflowDefinition> defs)
        throws ResourceConflictException
    {
        try {
            return defs.stream()
                .map(def -> {
                    try {
                        return store.insertWorkflowDefinition(project.getId(), revision.getId(), def, def.getTimeZone());
                    }
                    catch (ResourceConflictException ex) {
                        throw new IllegalStateException("Database state error", ex);
                    }
                })
                .collect(Collectors.toList());
        }
        catch (IllegalStateException ex) {
            ThrowablesUtil.propagateIfInstanceOf(ex.getCause(), ResourceConflictException.class);
            throw ex;
        }
    }

    @VisibleForTesting
    public void updateSchedules(
            StoredRevision revision, List<StoredWorkflowDefinition> defs, Map<String,Boolean> clearSchedules,
            SchedulerManager srm, Instant currentTime)
        throws ResourceConflictException
    {
        ImmutableList.Builder<ScheduleWithScheduler> schedules = ImmutableList.builder();
        for (StoredWorkflowDefinition def : defs) {
            Optional<Scheduler> sr = srm.tryGetScheduler(revision, def, true);
            if (sr.isPresent()) {
                ScheduleTime firstTime = sr.get().getFirstScheduleTime(currentTime);
                Schedule schedule = Schedule.of(def.getName(), def.getId(), firstTime.getRunTime(), firstTime.getTime());
                schedules.add(new ScheduleWithScheduler(schedule, sr.get()));
            }
        }

        // TODO validate workflows and sessions
        //   * compile workflow
        //   * validate SubtaskMatchPattern

        store.updateSchedules(project.getId(), schedules.build(), (oldStatus, newSched) -> {
            Optional<Instant> lastScheduleTime = oldStatus.getLastScheduleTime();
            boolean clearSchedule = clearSchedules.getOrDefault(newSched.getWorkflowName(), false);

            // if last schedule_time exists (the schedule has run before at least once) and no need to clear,
            // calculate next time from the last time
            ScheduleTime scheduleTime;
            if (lastScheduleTime.isPresent() && !clearSchedule) {
                scheduleTime = newSched.getScheduler().nextScheduleTime(lastScheduleTime.get());
            }
            // Otherwise, simply use the first execution time of the new schedule setting
            else {
                scheduleTime = ScheduleTime.of(newSched.getNextScheduleTime(), newSched.getNextRunTime());
            }
            return ScheduleTimeWithInfo.of(scheduleTime, clearSchedule);
        });
    }

    public void deleteSchedules()
    {
        store.deleteSchedules(project.getId());
    }

    private static class ScheduleWithScheduler
            extends Schedule
    {
        private Schedule schedule;
        private Scheduler scheduler;

        ScheduleWithScheduler(Schedule schedule, Scheduler scheduler)
        {
            this.schedule = schedule;
            this.scheduler = scheduler;
        }

        @Override
        public String getWorkflowName()
        {
            return schedule.getWorkflowName();
        }

        @Override
        public long getWorkflowDefinitionId()
        {
            return schedule.getWorkflowDefinitionId();
        }

        @Override
        public Instant getNextRunTime()
        {
            return schedule.getNextRunTime();
        }

        @Override
        public Instant getNextScheduleTime()
        {
            return schedule.getNextScheduleTime();
        }

        public Scheduler getScheduler()
        {
            return scheduler;
        }
    }
}
