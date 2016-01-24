package io.digdag.core.schedule;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.time.Instant;
import java.time.ZoneId;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import io.digdag.core.repository.RepositoryStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;
import io.digdag.core.workflow.TaskMatchPattern;
import io.digdag.core.workflow.SubtaskMatchPattern;
import io.digdag.core.session.SessionMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduleExecutor.class);

    private final RepositoryStoreManager rm;
    private final ScheduleStoreManager sm;
    private final SchedulerManager srm;
    private final ScheduleHandler handler;
    private final ExecutorService executor;

    @Inject
    public ScheduleExecutor(
            RepositoryStoreManager rm,
            ScheduleStoreManager sm,
            SchedulerManager srm,
            ScheduleHandler handler)
    {
        this.rm = rm;
        this.sm = sm;
        this.srm = srm;
        this.handler = handler;
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("scheduler-%d")
                .build()
                );
    }

    public void start()
    {
        executor.submit(() -> run());
    }

    public void run()
    {
        try {
            while (true) {
                Thread.sleep(1000);  // TODO sleep interval
                sm.lockReadySchedules(Instant.now(), (store, storedSchedule) -> {
                    schedule(new ScheduleControl(store, storedSchedule));
                });
            }
        }
        catch (Throwable t) {
            logger.error("Uncaught exception", t);
        }
    }

    // used by RepositoryControl.updateSchedules and startSchedule
    public static Optional<Config> getScheduleConfig(WorkflowDefinition def)
    {
        return def.getConfig().getOptional("schedule", Config.class);
    }

    public static ZoneId getWorkflowTimeZone(Config defaultParams, WorkflowDefinition def)
    {
        return def.getConfig().get("timezone", ZoneId.class,
                defaultParams.get("timezone", ZoneId.class,
                    ZoneId.of("UTC")));
    }

    public boolean schedule(ScheduleControl lockedSched)
    {
        StoredSchedule sched = lockedSched.get();

        // TODO If a workflow has wait-until-last-schedule attribute, don't start
        //      new session and return a ScheduleTime with delayed nextRunTime and
        //      same nextScheduleTime
        try {
            StoredWorkflowDefinitionWithRepository wf = rm.getWorkflowDetailsById(sched.getWorkflowDefinitionId());

            ZoneId timeZone = getWorkflowTimeZone(wf.getRevisionDefaultParams(), wf);
            Config schedConfig = getScheduleConfig(wf).get();

            Scheduler sr = srm.getScheduler(schedConfig, timeZone);

            try {
                ScheduleTime nextTime = startSchedule(sched, sr, wf);
                return lockedSched.tryUpdateNextScheduleTimeAndLastSessionInstant(nextTime, sched.getNextScheduleTime());
            }
            catch (ResourceConflictException ex) {
                Exception error = new IllegalStateException("Detected duplicated excution of a scheduled workflow for the same scheduling time.", ex);
                logger.error("Database state error during scheduling. Skipping this schedule", error);
                ScheduleTime nextTime = sr.nextScheduleTime(sched.getNextScheduleTime());
                return lockedSched.tryUpdateNextScheduleTime(nextTime);
            }
            catch (RuntimeException ex) {
                logger.error("Error during scheduling. Pending this schedule for 1 hour", ex);
                ScheduleTime nextTime = ScheduleTime.of(
                        sched.getNextRunTime().plusSeconds(3600),
                        sched.getNextScheduleTime());
                return lockedSched.tryUpdateNextScheduleTime(nextTime);
            }
        }
        catch (ResourceNotFoundException ex) {
            Exception error = new IllegalStateException("Workflow for a schedule id=" + sched.getId() + " is scheduled but does not exist.", ex);
            logger.error("Database state error during scheduling. Pending this schedule for 1 hour", error);
            ScheduleTime nextTime = ScheduleTime.of(
                    sched.getNextRunTime().plusSeconds(3600),
                    sched.getNextScheduleTime());
            return lockedSched.tryUpdateNextScheduleTime(nextTime);
        }
    }

    private ScheduleTime startSchedule(StoredSchedule sched, Scheduler sr,
            StoredWorkflowDefinitionWithRepository wf)
        throws ResourceNotFoundException, ResourceConflictException
    {
        Instant scheduleTime = sched.getNextScheduleTime();
        Instant runTime = sched.getNextRunTime();
        ZoneId timeZone = sr.getTimeZone();

        // TODO move this to WorkflowExecutor?
        ImmutableList.Builder<SessionMonitor> monitors = ImmutableList.builder();
        if (wf.getConfig().has("sla")) {
            Config slaConfig = wf.getConfig().getNestedOrGetEmpty("sla");
            // TODO support multiple SLAs
            Instant triggerTime = SlaCalculator.getTriggerTime(slaConfig, runTime, timeZone);
            monitors.add(SessionMonitor.of("sla", slaConfig, triggerTime));
        }

        handler.start(wf, monitors.build(),
                timeZone, ScheduleTime.of(runTime, scheduleTime));
        return sr.nextScheduleTime(scheduleTime);
    }

    public StoredSchedule skipScheduleToTime(int siteId, long schedId, Instant nextTime, Optional<Instant> runTime, boolean dryRun)
        throws ResourceNotFoundException, ResourceConflictException
    {
        sm.getScheduleStore(siteId).getScheduleById(schedId); // validastes siteId

        Optional<StoredSchedule> optional = sm.lockScheduleById(schedId, (store, sched) -> {
            ScheduleControl lockedSched = new ScheduleControl(store, sched);

            StoredWorkflowDefinitionWithRepository wf = rm.getWorkflowDetailsById(sched.getWorkflowDefinitionId());

            ZoneId timeZone = getWorkflowTimeZone(wf.getRevisionDefaultParams(), wf);
            Config schedConfig = getScheduleConfig(wf).get();

            Scheduler sr = srm.getScheduler(schedConfig, timeZone);

            ScheduleTime alignedNextTime = sr.getFirstScheduleTime(nextTime.minusSeconds(1));
            if (sched.getNextScheduleTime().isBefore(alignedNextTime.getScheduleTime())) {
                // OK
                if (runTime.isPresent()) {
                    alignedNextTime = ScheduleTime.of(runTime.get(), alignedNextTime.getScheduleTime());
                }
                if (!dryRun) {
                    sched = lockedSched.updateNextScheduleTime(alignedNextTime); // TODO validate return value is true, otherwise throw ResourceConflictException
                }
                return Optional.of(sched);  // return updated StoredSchedule
            }
            else {
                // NG
                return Optional.<StoredSchedule>absent();
            }
        });
        if (optional.isPresent()) {
            return optional.get();
        }
        else {
            throw new ResourceConflictException("Specified time to skip schedules is already past");
        }
    }

    public StoredSchedule skipScheduleByCount(int siteId, long schedId, Instant currentTime, int count, Optional<Instant> runTime, boolean dryRun)
        throws ResourceNotFoundException, ResourceConflictException
    {
        sm.getScheduleStore(siteId).getScheduleById(schedId); // validastes siteId

        Optional<StoredSchedule> optional = sm.lockScheduleById(schedId, (store, sched) -> {
            ScheduleControl lockedSched = new ScheduleControl(store, sched);

            StoredWorkflowDefinitionWithRepository wf = rm.getWorkflowDetailsById(sched.getWorkflowDefinitionId());

            ZoneId timeZone = getWorkflowTimeZone(wf.getRevisionDefaultParams(), wf);
            Config schedConfig = getScheduleConfig(wf).get();

            Scheduler sr = srm.getScheduler(schedConfig, timeZone);

            ScheduleTime time = sr.getFirstScheduleTime(currentTime);
            for (int i=0; i < count; i++) {
                time = sr.nextScheduleTime(time.getScheduleTime());
            }
            if (sched.getNextScheduleTime().isBefore(time.getScheduleTime())) {
                // OK
                if (runTime.isPresent()) {
                    time = ScheduleTime.of(runTime.get(), time.getScheduleTime());
                }
                if (!dryRun) {
                    sched = lockedSched.updateNextScheduleTime(time); // TODO validate return value is true, otherwise throw ResourceConflictException
                }
                return Optional.of(sched);
            }
            else {
                // NG
                return Optional.<StoredSchedule>absent();
            }
        });
        if (optional.isPresent()) {
            return optional.get();
        }
        else {
            throw new ResourceConflictException("Specified time to skip schedules is already past");
        }
    }
}
