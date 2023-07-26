package io.digdag.core.database;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.core.acroute.DefaultAccountRoutingFactory;
import io.digdag.core.repository.Project;
import io.digdag.core.repository.ProjectControl;
import io.digdag.core.repository.ProjectControlStore.ScheduleTimeWithInfo;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.Revision;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.schedule.Schedule;
import io.digdag.core.schedule.ScheduleStore;
import io.digdag.core.schedule.ScheduleStoreManager;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.schedule.StoredSchedule;
import io.digdag.spi.AccountRouting;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.skife.jdbi.v2.Handle;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.database.DatabaseTestingUtils.assertNotFound;
import static io.digdag.core.database.DatabaseTestingUtils.createRevision;
import static io.digdag.core.database.DatabaseTestingUtils.createWorkflow;
import static io.digdag.core.database.DatabaseTestingUtils.setupDatabase;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class DatabaseScheduleStoreManagerTest
{
    private DatabaseFactory factory;
    private ProjectStoreManager manager;
    private ProjectStore store;

    private ScheduleStoreManager schedManager;
    private ScheduleStore schedStore;

    private AccountRouting accountRoutingDisabled;
    private AccountRouting accountRoutingInclude0; // account_routing.include = 0;
    private AccountRouting accountRoutingExclude0; // account_routing.exclude = 0;

    @Before
    public void setUp()
            throws Exception
    {
        factory = setupDatabase();
        factory.begin(() -> {
            manager = factory.getProjectStoreManager();
            store = manager.getProjectStore(0);
            schedManager = factory.getScheduleStoreManager();
            schedStore = schedManager.getScheduleStore(0);
        });
        setUpAccountRouting();
    }

    private void setUpAccountRouting()
    {
        Config cf1 = newConfig();
        this.accountRoutingDisabled = DefaultAccountRoutingFactory.fromConfig(cf1, Optional.of(AccountRouting.ModuleType.SCHEDULER.toString()));
        cf1.set("scheduler.account_routing.enabled", "true")
                .set("scheduler.account_routing.include", "0");
        this.accountRoutingInclude0 = DefaultAccountRoutingFactory.fromConfig(cf1, Optional.of(AccountRouting.ModuleType.SCHEDULER.toString()));
        cf1.remove("scheduler.account_routing.include")
                .set("scheduler.account_routing.exclude", "0");
        this.accountRoutingExclude0 = DefaultAccountRoutingFactory.fromConfig(cf1, Optional.of(AccountRouting.ModuleType.SCHEDULER.toString()));
    }

    @After
    public void destroy()
    {
        factory.close();
    }

    @Test
    public void testGetAndNotFounds()
        throws Exception
    {
        factory.begin(() -> {
            Project srcProj1 = Project.of("proj1");
            Revision srcRev1 = createRevision("rev1");
            WorkflowDefinition srcWf1Rev1 = createWorkflow("wf1");
            WorkflowDefinition srcWf2 = createWorkflow("wf2");

            Revision srcRev2 = createRevision("rev2");
            WorkflowDefinition srcWf1Rev2 = createWorkflow("wf1");
            WorkflowDefinition srcWf3 = createWorkflow("wf3");

            Instant now = Instant.ofEpochSecond(Instant.now().getEpochSecond());
            Instant runTime1 = now.plusSeconds(1);
            Instant schedTime1 = now.plusSeconds(10);
            Instant runTime2 = now.plusSeconds(2);
            Instant schedTime2 = now.plusSeconds(20);

            final AtomicReference<StoredRevision> revRef = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRefA = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRefB = new AtomicReference<>();

            StoredProject proj1 = store.putAndLockProject(
                    srcProj1,
                    (store, stored) -> {
                        ProjectControl lock = new ProjectControl(store, stored);
                        revRef.set(lock.insertRevision(srcRev1));
                        wfRefA.set(lock.insertWorkflowDefinitionsWithoutSchedules(revRef.get(), ImmutableList.of(srcWf1Rev1)).get(0));
                        wfRefB.set(lock.insertWorkflowDefinitionsWithoutSchedules(revRef.get(), ImmutableList.of(srcWf2)).get(0));
                        store.updateSchedules(
                                stored.getId(),
                                ImmutableList.of(
                                        Schedule.of(
                                                srcWf1Rev1.getName(),
                                                wfRefA.get().getId(),
                                                runTime1,
                                                schedTime1),
                                        Schedule.of(
                                                srcWf2.getName(),
                                                wfRefB.get().getId(),
                                                runTime1,
                                                schedTime1)
                                ),
                                (oldStatus, newSched) -> ScheduleTimeWithInfo.of(oldStatus.getNextScheduleTime()));
                        return lock.get();
                    });
            StoredWorkflowDefinition wf1Rev1 = wfRefA.get();
            StoredWorkflowDefinition wf2 = wfRefB.get();
            List<StoredSchedule> schedList1 = schedStore.getSchedules(100, Optional.absent(), () -> "true");
            assertEquals(2, schedList1.size());
            StoredSchedule sched1 = schedList1.get(0);
            StoredSchedule sched2 = schedList1.get(1);

            assertThat(schedStore.getSchedulesByProjectId(proj1.getId(), 100, Optional.absent(), () -> "true"), is(schedList1));
            assertThat(schedStore.getScheduleByProjectIdAndWorkflowName(proj1.getId(), srcWf1Rev1.getName()), is(sched1));
            assertThat(schedStore.getScheduleByProjectIdAndWorkflowName(proj1.getId(), srcWf2.getName()), is(sched2));

            assertThat(schedStore.getSchedulesByProjectId(4711, 100, Optional.absent(), () -> "true"), is(empty()));

            try {
                schedStore.getScheduleByProjectIdAndWorkflowName(proj1.getId(), "non-existent-workflow");
                fail();
            }
            catch (ResourceNotFoundException ignore) {
            }

            try {
                schedStore.getScheduleByProjectIdAndWorkflowName(4711, srcWf1Rev1.getName());
                fail();
            }
            catch (ResourceNotFoundException ignore) {
            }

            store.putAndLockProject(
                    srcProj1,
                    (store, stored) -> {
                        ProjectControl lock = new ProjectControl(store, stored);
                        revRef.set(lock.insertRevision(srcRev2));
                        wfRefA.set(lock.insertWorkflowDefinitionsWithoutSchedules(revRef.get(), ImmutableList.of(srcWf1Rev2)).get(0));
                        wfRefB.set(lock.insertWorkflowDefinitionsWithoutSchedules(revRef.get(), ImmutableList.of(srcWf3)).get(0));
                        store.updateSchedules(
                                stored.getId(),
                                ImmutableList.of(
                                        Schedule.of(
                                                srcWf1Rev2.getName(),
                                                wfRefA.get().getId(),
                                                runTime1,
                                                schedTime2),
                                        Schedule.of(
                                                srcWf3.getName(),
                                                wfRefB.get().getId(),
                                                runTime2,
                                                schedTime2)
                                ),
                                (oldStatus, newSched) -> {
                                    // when conflicted (wf1), rollback 60 seconds schedule time and 120 seconds run time here
                                    return ScheduleTimeWithInfo.of(ScheduleTime.of(
                                            oldStatus.getNextScheduleTime().getTime().minusSeconds(60),
                                            oldStatus.getNextScheduleTime().getRunTime().minusSeconds(120)));
                                });
                        return lock.get();
                    });
            StoredWorkflowDefinition wf1Rev2 = wfRefA.get();
            StoredWorkflowDefinition wf3 = wfRefB.get();
            List<StoredSchedule> schedList2 = schedStore.getSchedules(100, Optional.absent(), () -> "true");
            assertEquals(2, schedList2.size());
            StoredSchedule sched3 = schedList2.get(0);
            StoredSchedule sched4 = schedList2.get(1);

            ////
            // return value of setters
            //
            assertEquals(proj1.getId(), sched1.getProjectId());
            assertEquals(proj1.getId(), sched2.getProjectId());
            assertEquals(wf1Rev1.getName(), sched1.getWorkflowName());
            assertEquals(wf2.getName(), sched2.getWorkflowName());
            assertEquals(wf1Rev1.getId(), sched1.getWorkflowDefinitionId());
            assertEquals(wf2.getId(), sched2.getWorkflowDefinitionId());

            assertEquals(proj1.getId(), sched3.getProjectId());
            assertEquals(proj1.getId(), sched4.getProjectId());
            assertEquals(wf1Rev2.getName(), sched3.getWorkflowName());
            assertEquals(wf3.getName(), sched4.getWorkflowName());
            assertEquals(wf1Rev2.getId(), sched3.getWorkflowDefinitionId());
            assertEquals(wf3.getId(), sched4.getWorkflowDefinitionId());

            ////
            // test run time
            //
            assertEquals(runTime1, sched1.getNextRunTime());
            assertEquals(runTime1, sched2.getNextRunTime());
            assertEquals(schedTime1, sched1.getNextScheduleTime());
            assertEquals(schedTime1, sched2.getNextScheduleTime());

            assertEquals(runTime1.minusSeconds(120), sched3.getNextRunTime());   // conflicted, 2 minute rollback
            assertEquals(runTime2, sched4.getNextRunTime());
            assertEquals(schedTime1.minusSeconds(60), sched3.getNextScheduleTime());  // conflicted, 1 minutes rollback
            assertEquals(schedTime2, sched4.getNextScheduleTime());

            ////
            // public simple getters
            //
            assertEquals(sched3.getId(), sched1.getId());  // sched1 is overwritten by rev2 == sched3
            assertNotFound(() -> schedStore.getScheduleById(sched2.getId()));   // sched2 is deleted
            assertEquals(sched3, schedStore.getScheduleById(sched3.getId()));
            assertEquals(sched4, schedStore.getScheduleById(sched4.getId()));

            ////
            // control actions
            //
            assertEquals(sched4.getId(), (long) schedStore.lockScheduleById(sched4.getId(), (store, schedule) -> schedule.getId()));

            List<Integer> lockedByRuntime1 = new ArrayList<>();
            schedManager.lockReadySchedules(runTime1, 10, accountRoutingDisabled, (store, schedule) -> {
                lockedByRuntime1.add(schedule.getId());
            });
            assertEquals(ImmutableList.of(sched1.getId()), lockedByRuntime1);

            List<Integer> lockedByRuntime2 = new ArrayList<>();
            schedManager.lockReadySchedules(runTime2, 10, accountRoutingDisabled, (store, schedule) -> {
                lockedByRuntime2.add(schedule.getId());
            });
            assertEquals(ImmutableList.of(sched3.getId(), sched4.getId()), lockedByRuntime2);

            // exception during lockReadySchedules
            try {
                schedManager.lockReadySchedules(runTime2, 10, accountRoutingDisabled, (store, schedule) -> {
                    throw new RuntimeException("processing " + schedule.getId());
                });
                fail();
            }
            catch (RuntimeException ex) {
                assertEquals(ex.getMessage(), "processing " + sched3.getId());
                assertEquals(1, ex.getSuppressed().length);
                assertEquals(ex.getSuppressed()[0].getMessage(), "processing " + sched4.getId());
            }

            // partial processing of lockReadySchedules
            Instant runTime3 = now.plusSeconds(3);
            Instant schedTime3 = now.plusSeconds(30);
            Instant runTime4 = now.plusSeconds(4);
            Instant schedTime4 = now.plusSeconds(40);

            try {
                schedManager.lockReadySchedules(runTime2, 10, accountRoutingDisabled, (store, schedule) -> {
                    if (schedule.getId() == sched3.getId()) {
                        throw new RuntimeException();
                    }
                    else {
                        try {
                            store.updateNextScheduleTime(schedule.getId(), ScheduleTime.of(schedTime3, runTime3));
                        }
                        catch (ResourceNotFoundException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                });
                fail();
            }
            catch (RuntimeException ex) {
            }

            List<Integer> updated = new ArrayList<>();
            schedManager.lockReadySchedules(runTime2, 10, accountRoutingDisabled, (store, schedule) -> {
                updated.add(schedule.getId());
                try {
                    store.updateNextScheduleTimeAndLastSessionTime(schedule.getId(), ScheduleTime.of(schedTime4, runTime4), schedTime1);
                }
                catch (ResourceNotFoundException ex) {
                    throw new RuntimeException(ex);
                }
            });
            assertEquals(ImmutableList.of(sched1.getId()), updated);

            StoredSchedule updatedSched1 = schedStore.getSchedules(100, Optional.absent(), () -> "true").get(0);
            assertEquals(sched1.getId(), updatedSched1.getId());
            assertEquals(runTime4, updatedSched1.getNextRunTime());
            assertEquals(schedTime4, updatedSched1.getNextScheduleTime());
        });
    }

    @Test
    public void testDisableEnable()
        throws Exception
    {
        factory.begin(() -> {
            Project srcProj1 = Project.of("proj1");
            Revision srcRev1 = createRevision("rev1");
            WorkflowDefinition srcWf1Rev1 = createWorkflow("wf1");
            WorkflowDefinition srcWf2 = createWorkflow("wf2");

            Instant yesterday = Instant.now().minus(Duration.ofDays(1)).truncatedTo(SECONDS);

            final AtomicReference<StoredRevision> revRef = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRefA = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRefB = new AtomicReference<>();

            StoredProject proj1 = store.putAndLockProject(
                    srcProj1,
                    (store, stored) -> {
                        ProjectControl lock = new ProjectControl(store, stored);
                        revRef.set(lock.insertRevision(srcRev1));
                        wfRefA.set(lock.insertWorkflowDefinitionsWithoutSchedules(revRef.get(), ImmutableList.of(srcWf1Rev1)).get(0));
                        wfRefB.set(lock.insertWorkflowDefinitionsWithoutSchedules(revRef.get(), ImmutableList.of(srcWf2)).get(0));
                        store.updateSchedules(
                                stored.getId(),
                                ImmutableList.of(
                                        Schedule.of(
                                                srcWf1Rev1.getName(),
                                                wfRefA.get().getId(),
                                                yesterday,
                                                yesterday),
                                        Schedule.of(
                                                srcWf2.getName(),
                                                wfRefB.get().getId(),
                                                yesterday,
                                                yesterday)
                                ),
                                (oldStatus, newSched) -> ScheduleTimeWithInfo.of(oldStatus.getNextScheduleTime()));
                        return lock.get();
                    });

            StoredWorkflowDefinition wf1 = wfRefA.get();
            StoredWorkflowDefinition wf2 = wfRefB.get();
            List<StoredSchedule> schedList1 = schedStore.getSchedules(100, Optional.absent(), () -> "true");
            assertEquals(2, schedList1.size());
            StoredSchedule sched1 = schedList1.get(0);
            StoredSchedule sched2 = schedList1.get(1);

            // Verify that enabling a schedule that has not been disabled is a nop
            schedStore.updateScheduleById(sched1.getId(), (store, schedule) -> {
                store.enableSchedule(schedule.getId());
                return schedule;
            });
            {
                List<Integer> ready = new ArrayList<>();
                schedManager.lockReadySchedules(Instant.now(), 10, accountRoutingDisabled, (store, schedule) -> ready.add(schedule.getId()));
                assertThat(ready, containsInAnyOrder(sched1.getId(), sched2.getId()));
            }

            // Disable one of the schedules and verify that lockReadySchedules skips it
            schedStore.updateScheduleById(sched1.getId(), (store, schedule) -> {
                store.disableSchedule(schedule.getId());
                return schedule;
            });
            {
                List<Integer> ready = new ArrayList<>();
                schedManager.lockReadySchedules(Instant.now(), 10, accountRoutingDisabled, (store, schedule) -> ready.add(schedule.getId()));
                assertThat(ready, contains(sched2.getId()));
            }

            // Verify that the disabled schedule can still be fetched
            {
                double now = Instant.now().getEpochSecond();

                StoredSchedule s1 = schedStore.getScheduleByProjectIdAndWorkflowName(proj1.getId(), wf1.getName());
                StoredSchedule s2 = schedStore.getScheduleByProjectIdAndWorkflowName(proj1.getId(), wf2.getName());
                assertThat(s1.getId(), is(sched1.getId()));
                assertThat(s2.getId(), is(sched2.getId()));
                assertThat((double) s1.getDisabledAt().get().getEpochSecond(), is(closeTo(now, 30)));
                assertThat(s2.getDisabledAt(), is(Optional.absent()));

                assertThat(schedStore.getSchedules(100, Optional.absent(), () -> "true"),
                        containsInAnyOrder(s1, s2));
                assertThat(schedStore.getSchedulesByProjectId(proj1.getId(), 100, Optional.absent(), () -> "true"),
                        containsInAnyOrder(s1, s2));
            }

            // Re-enable the schedule and verify that lockReadySchedules processes it
            schedStore.updateScheduleById(sched1.getId(), (store, schedule) -> {
                store.enableSchedule(schedule.getId());
                return schedule;
            });
            {
                List<Integer> ready = new ArrayList<>();
                schedManager.lockReadySchedules(Instant.now(), 10, accountRoutingDisabled, (store, schedule) -> ready.add(schedule.getId()));
                assertThat(ready, containsInAnyOrder(sched1.getId(), sched2.getId()));
            }

            // Verify that enabling is idempotent
            schedStore.updateScheduleById(sched1.getId(), (store, schedule) -> {
                store.enableSchedule(schedule.getId());
                return schedule;
            });
            {
                List<Integer> ready = new ArrayList<>();
                schedManager.lockReadySchedules(Instant.now(), 10, accountRoutingDisabled, (store, schedule) -> ready.add(schedule.getId()));
                assertThat(ready, containsInAnyOrder(sched1.getId(), sched2.getId()));
            }
        });
    }

    @Test
    public void testAccountRouting()
            throws Exception
    {
        factory.begin(() -> {
            Project srcProj1 = Project.of("proj1");
            Revision srcRev1 = createRevision("rev1");
            WorkflowDefinition srcWf1Rev1 = createWorkflow("wf1");
            WorkflowDefinition srcWf2 = createWorkflow("wf2");

            Instant yesterday = Instant.now().minus(Duration.ofDays(1)).truncatedTo(SECONDS);

            final AtomicReference<StoredRevision> revRef = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRefA = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRefB = new AtomicReference<>();

            StoredProject proj1 = store.putAndLockProject(
                    srcProj1,
                    (store, stored) -> {
                        ProjectControl lock = new ProjectControl(store, stored);
                        revRef.set(lock.insertRevision(srcRev1));
                        wfRefA.set(lock.insertWorkflowDefinitionsWithoutSchedules(revRef.get(), ImmutableList.of(srcWf1Rev1)).get(0));
                        wfRefB.set(lock.insertWorkflowDefinitionsWithoutSchedules(revRef.get(), ImmutableList.of(srcWf2)).get(0));
                        store.updateSchedules(
                                stored.getId(),
                                ImmutableList.of(
                                        Schedule.of(
                                                srcWf1Rev1.getName(),
                                                wfRefA.get().getId(),
                                                yesterday,
                                                yesterday),
                                        Schedule.of(
                                                srcWf2.getName(),
                                                wfRefB.get().getId(),
                                                yesterday,
                                                yesterday)
                                ),
                                (oldStatus, newSched) -> ScheduleTimeWithInfo.of(oldStatus.getNextScheduleTime()));
                        return lock.get();
                    });

            List<StoredSchedule> schedList1 = schedStore.getSchedules(100, Optional.absent(), () -> "true");
            assertEquals(2, schedList1.size());

            {
                List<Integer> ready = new ArrayList<>();
                schedManager.lockReadySchedules(Instant.now(), 10, accountRoutingDisabled, (store, schedule) -> ready.add(schedule.getId()));
                assertThat(ready.size(), is(2));
            }
            {
                List<Integer> ready = new ArrayList<>();
                schedManager.lockReadySchedules(Instant.now(), 10, accountRoutingInclude0, (store, schedule) -> ready.add(schedule.getId()));
                assertThat(ready.size(), is(2));
            }
            {
                List<Integer> ready = new ArrayList<>();
                schedManager.lockReadySchedules(Instant.now(), 10, accountRoutingExclude0, (store, schedule) -> ready.add(schedule.getId()));
                assertThat(ready.size(), is(0));
            }
        });
    }

    @Test
    public void testUpdateSchedulesWithClearSchedules()
            throws Exception {
        final Instant firstTime = Instant.parse("2022-05-15T13:00:00.00Z");
        final Instant nextTime = Instant.parse("2022-05-30T13:00:00.00Z");

        SchedulerManager sm = mock(SchedulerManager.class);
        setUpScheduler(firstTime, nextTime, sm); // mock SchedulerManager to return dummy Scheduler

        final ConfigMapper cfm = ((DatabaseProjectStoreManager) manager).configMapper;

        factory.get().begin(() -> {
            Handle handle = factory.get().getHandle(cfm);

            Project srcProj1 = Project.of("proj1");
            Revision srcRev1 = createRevision("rev1");
            WorkflowDefinition srcWf1Rev1 = createWorkflow("wfA");
            WorkflowDefinition srcWf2 = createWorkflow("wfB");

            final AtomicReference<StoredRevision> revRef = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRefA = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRefB = new AtomicReference<>();

            Instant now = Instant.now();
            try {
                StoredProject proj1 = store.putAndLockProject(
                        srcProj1,
                        (store, stored) -> {
                            // prepare for workflow
                            ProjectControl lock = new ProjectControl(store, stored);
                            revRef.set(lock.insertRevision(srcRev1));
                            wfRefA.set(lock.insertWorkflowDefinitionsWithoutSchedules(revRef.get(), ImmutableList.of(srcWf1Rev1)).get(0));
                            wfRefB.set(lock.insertWorkflowDefinitionsWithoutSchedules(revRef.get(), ImmutableList.of(srcWf2)).get(0));
                            StoredWorkflowDefinition wfA = wfRefA.get();
                            StoredWorkflowDefinition wfB = wfRefB.get();
                            List<StoredWorkflowDefinition> wfAll = Arrays.asList(wfA, wfB);
                            // update schedules table initially.
                            lock.updateSchedules(revRef.get(), wfAll, Collections.emptyMap(), sm, Instant.now());

                            // get schedule
                            StoredSchedule schdA = getScheduleByWorkflow(handle, wfA.getId());
                            StoredSchedule schdB = getScheduleByWorkflow(handle, wfB.getId());

                            // check schedule. This is first one. So calculate from getFirstScheduleTime
                            assertThat(schdA.getNextScheduleTime(), is(firstTime));
                            assertThat(schdB.getNextScheduleTime(), is(firstTime));

                            // set last_session_time of the schedules
                            updateLastSessionTime(handle, schdA.getId(), now);
                            updateLastSessionTime(handle, schdB.getId(), now);
                            // update schedules table again.
                            lock.updateSchedules(revRef.get(), wfAll, Collections.emptyMap(), sm, Instant.now());
                            schdA = getScheduleByWorkflow(handle, wfA.getId());
                            schdB = getScheduleByWorkflow(handle, wfB.getId());
                            // check schedule. calculated based on last_session_time
                            assertThat(schdA.getNextScheduleTime(), is(nextTime));
                            assertThat(schdB.getNextScheduleTime(), is(nextTime));

                            // update schedule table with clearSchedules("wfA" -> true).
                            lock.updateSchedules(revRef.get(), wfAll, Collections.singletonMap("wfA", true), sm, Instant.now());
                            schdA = getScheduleByWorkflow(handle, wfA.getId());
                            schdB = getScheduleByWorkflow(handle, wfB.getId());
                            // check schedule.
                            // wfA: last_session_time is cleared. So calculated from getFirstScheduleTime
                            // wfB: last_session_time is not cleared. So calculated based on last_session_time
                            assertThat(schdA.getNextScheduleTime(), is(firstTime));
                            assertThat(schdB.getNextScheduleTime(), is(nextTime));

                            return lock.get();
                        });
            }
            catch (ResourceConflictException rce) {
                rce.printStackTrace();
                fail(rce.toString());
            }
            return 0;
        });
    }

    private static void setUpScheduler(Instant firstTime, Instant nextTime, SchedulerManager sm)
            throws Exception {
        doReturn(Optional.of(new Scheduler() {

            @Override
            public ZoneId getTimeZone() {
                return ZoneId.of("utc");
            }

            @Override
            public ScheduleTime getFirstScheduleTime(Instant currentTime) {
                return ScheduleTime.of(firstTime, firstTime);
            }

            @Override
            public ScheduleTime nextScheduleTime(Instant lastScheduleTime) {
                return ScheduleTime.of(nextTime, nextTime);
            }

            @Override
            public ScheduleTime lastScheduleTime(Instant currentScheduleTime) {
                return ScheduleTime.of(currentScheduleTime, currentScheduleTime);
            }
        }))
                .when(sm).tryGetScheduler(any(Revision.class), any(WorkflowDefinition.class), any(Boolean.class));
    }

    private static int updateLastSessionTime(Handle handle, int scheduleId, Instant lastSessionTime)
    {
        return handle.createStatement("update schedules set last_session_time = :lastSessionTime, updated_at = now() where id = :id")
                .bind("lastSessionTime", lastSessionTime.getEpochSecond())
                .bind("id", scheduleId)
                .execute();
    }

    private static StoredSchedule getScheduleByWorkflow(Handle handle, long id)
    {
        List<StoredSchedule> schdLists = handle.createQuery("select *, wd.name as name from schedules s join workflow_definitions wd on wd.id = s.workflow_definition_id where workflow_definition_id = :wfid")
                .bind("wfid", id)
                .mapTo(StoredSchedule.class).list();
        assertThat(schdLists, hasSize(1));
        return schdLists.get(0);
    }
}
