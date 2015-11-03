package io.digdag.core;

import java.util.List;
import java.util.Date;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Optional;
import io.digdag.core.config.Config;

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

    public StoredWorkflowSource putWorkflow(int revId, WorkflowSource workflow)
    {
        return store.putWorkflow(revId, workflow);
    }

    public void syncSchedulesTo(ScheduleStore schedStore, SchedulerManager scheds,
            Date currentTime, StoredRevision revision)  // TODO here needs ScheduleOptions
    {
        ImmutableList.Builder<Schedule> schedules = ImmutableList.builder();
        Optional<Integer> lastId = Optional.absent();
        while (true) {
            List<StoredWorkflowSource> wfs = store.getWorkflows(revision.getId(), 100, lastId);
            if (wfs.isEmpty()) {
                break;
            }
            for (StoredWorkflowSource wf : wfs) {
                Optional<Config> schedulerConfig = scheds.getSchedulerConfig(wf);
                if (schedulerConfig.isPresent()) {
                    Scheduler sr = scheds.getScheduler(schedulerConfig.get());
                    ScheduleTime firstTime = sr.getFirstScheduleTime(currentTime);
                    Schedule schedule = Schedule.of(wf.getId(), schedulerConfig.get(),
                            firstTime.getRunTime(), firstTime.getScheduleTime());
                    schedules.add(schedule);
                }
            }
            lastId = Optional.of(wfs.get(wfs.size() - 1).getId());
        }
        schedStore.syncRepositorySchedules(revision.getRepositoryId(), schedules.build());
    }
}
