package io.digdag.core;

import java.util.List;
import java.util.Date;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Optional;

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
            Date currentTime, StoredRevision revision)
    {
        ImmutableList.Builder<Schedule> schedules = ImmutableList.builder();
        Optional<Integer> lastId = Optional.absent();
        while (true) {
            List<StoredWorkflowSource> wfs = store.getWorkflows(revision.getId(), 100, lastId);
            if (wfs.isEmpty()) {
                break;
            }
            for (StoredWorkflowSource wf : wfs) {
                ConfigSource config = wf.getConfig().getNestedOrGetEmpty("schedule");
                if (config.isEmpty()) {
                    continue;
                }
                ScheduleTime firstTime = scheds.getScheduler(config).getFirstScheduleTime(currentTime);
                Schedule schedule = Schedule.scheduleBuilder()
                    .workflowId(wf.getId())
                    .config(config)
                    .nextRunTime(firstTime.getNextRunTime())
                    .nextScheduleTime(firstTime.getNextScheduleTime())
                    .build();
                schedules.add(schedule);
            }
            lastId = Optional.of(wfs.get(wfs.size() - 1).getId());
        }
        schedStore.syncRepositorySchedules(revision.getRepositoryId(), schedules.build());
    }
}
