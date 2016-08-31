package io.digdag.core.database;

import java.util.*;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import org.skife.jdbi.v2.IDBI;
import org.junit.*;
import com.google.common.base.Optional;
import com.google.common.collect.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.spi.ScheduleTime;
import static io.digdag.core.database.DatabaseTestingUtils.*;
import static org.junit.Assert.*;

public class DatabaseScheduleStoreManagerTest
{
    private DatabaseFactory factory;
    private ProjectStoreManager manager;
    private ProjectStore store;

    private ScheduleStoreManager schedManager;
    private ScheduleStore schedStore;

    @Before
    public void setUp()
    {
        factory = setupDatabase();
        manager = factory.getProjectStoreManager();
        store = manager.getProjectStore(0);
        schedManager = factory.getScheduleStoreManager();
        schedStore = schedManager.getScheduleStore(0);
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
                            (oldStatus, newSched) -> {
                                return oldStatus.getNextScheduleTime();
                            });
                    return lock.get();
                });
        StoredRevision rev1 = revRef.get();
        StoredWorkflowDefinition wf1Rev1 = wfRefA.get();
        StoredWorkflowDefinition wf2 = wfRefB.get();
        List<StoredSchedule> schedList1 = schedStore.getSchedules(100, Optional.absent());
        assertEquals(2, schedList1.size());
        StoredSchedule sched1 = schedList1.get(0);
        StoredSchedule sched2 = schedList1.get(1);

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
                                return ScheduleTime.of(
                                        oldStatus.getNextScheduleTime().getTime().minusSeconds(60),
                                        oldStatus.getNextScheduleTime().getRunTime().minusSeconds(120));
                            });
                    return lock.get();
                });
        StoredRevision rev2 = revRef.get();
        StoredWorkflowDefinition wf1Rev2 = wfRefA.get();
        StoredWorkflowDefinition wf3 = wfRefB.get();
        List<StoredSchedule> schedList2 = schedStore.getSchedules(100, Optional.absent());
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
        // manager internal getters
        //
        assertEquals(sched4.getId(), (long) schedManager.lockScheduleById(sched4.getId(), (store, schedule) -> schedule.getId()));

        List<Integer> lockedByRuntime1 = new ArrayList<>();
        schedManager.lockReadySchedules(runTime1, (store, schedule) -> {
            lockedByRuntime1.add(schedule.getId());
        });
        assertEquals(ImmutableList.of(sched1.getId()), lockedByRuntime1);

        List<Integer> lockedByRuntime2 = new ArrayList<>();
        schedManager.lockReadySchedules(runTime2, (store, schedule) -> {
            lockedByRuntime2.add(schedule.getId());
        });
        assertEquals(ImmutableList.of(sched3.getId(), sched4.getId()), lockedByRuntime2);

        // exception during lockReadySchedules
        try {
            schedManager.lockReadySchedules(runTime2, (store, schedule) -> {
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
            schedManager.lockReadySchedules(runTime2, (store, schedule) -> {
                if (schedule.getId() == sched3.getId()) {
                    throw new RuntimeException();
                }
                else {
                    store.updateNextScheduleTime(schedule.getId(), ScheduleTime.of(schedTime3, runTime3));
                }
            });
            fail();
        }
        catch (RuntimeException ex) {
        }

        List<Integer> updated = new ArrayList<>();
        schedManager.lockReadySchedules(runTime2, (store, schedule) -> {
            updated.add(schedule.getId());
            store.updateNextScheduleTime(schedule.getId(), ScheduleTime.of(schedTime4, runTime4), schedTime1);
        });
        assertEquals(ImmutableList.of(sched1.getId()), updated);

        StoredSchedule updatedSched1 = schedStore.getSchedules(100, Optional.absent()).get(0);
        assertEquals(sched1.getId(), updatedSched1.getId());
        assertEquals(runTime4, updatedSched1.getNextRunTime());
        assertEquals(schedTime4, updatedSched1.getNextScheduleTime());
    }
}
