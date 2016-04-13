package io.digdag.core.database;

import java.util.*;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;
import org.skife.jdbi.v2.IDBI;
import org.junit.*;
import com.google.common.base.Optional;
import com.google.common.collect.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.core.session.*;
import io.digdag.core.workflow.*;
import io.digdag.spi.ScheduleTime;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import static io.digdag.core.database.DatabaseTestingUtils.*;
import static org.junit.Assert.*;

public class DatabaseSessionStoreManagerTest
{
    private DatabaseFactory factory;
    private ProjectStoreManager projectStoreManager;
    private ProjectStore projectStore;

    private SessionStoreManager manager;
    private SessionStore store;

    private ConfigFactory cf = createConfigFactory();
    private WorkflowExecutor exec;
    private AttemptBuilder attemptBuilder;

    private StoredProject proj;
    private StoredRevision rev;
    private StoredWorkflowDefinition wf1;
    private StoredWorkflowDefinition wf2;

    @Before
    public void setUp()
        throws Exception
    {
        factory = setupDatabase();
        projectStoreManager = factory.getProjectStoreManager();
        projectStore = projectStoreManager.getProjectStore(0);
        manager = factory.getSessionStoreManager();
        store = manager.getSessionStore(0);

        exec = factory.getWorkflowExecutor();

        Project srcProj = Project.of("repo1");
        Revision srcRev = createRevision("rev1");
        WorkflowDefinition srcWf1 = createWorkflow("+wf1");
        WorkflowDefinition srcWf2 = createWorkflow("+wf2");

        attemptBuilder = new AttemptBuilder(
                new SchedulerManager(ImmutableSet.of()),
                new SlaCalculator());

        proj = projectStore.putAndLockProject(
                srcProj,
                (store, stored) -> {
                    ProjectControl lock = new ProjectControl(store, stored);

                    rev = lock.insertRevision(srcRev);

                    List<StoredWorkflowDefinition> storedWfs = lock.insertWorkflowDefinitionsWithoutSchedules(rev, ImmutableList.of(srcWf1, srcWf2));
                    wf1 = storedWfs.get(0);
                    wf2 = storedWfs.get(1);
                    return lock.get();
                });
    }

    @After
    public void destroy()
    {
        factory.close();
    }

    @Test
    public void testConflicts()
        throws Exception
    {
        Instant sessionTime1 = Instant.ofEpochSecond(Instant.now().getEpochSecond() / 3600 * 3600);

        AttemptRequest ar1 = attemptBuilder.buildFromStoredWorkflow(
                rev,
                wf1,
                cf.create(),
                ScheduleTime.runNow(sessionTime1),
                Optional.absent());

        exec.submitWorkflow(0, ar1, wf1);

        // same session conflicts
        assertConflict(() -> {
            propagateOnly(ResourceConflictException.class, () ->
                exec.submitWorkflow(0, ar1, wf1)
            );
        });

        // different session params conflicts
        assertConflict(() -> {
            propagateOnly(ResourceConflictException.class, () ->
                exec.submitWorkflow(0,
                    ImmutableAttemptRequest.builder().from(ar1)
                    .sessionParams(cf.create().set("a", 1))
                    .build(),
                    wf1)
            );
        });

        // different timezone conflicts
        assertConflict(() -> {
            propagateOnly(ResourceConflictException.class, () ->
                exec.submitWorkflow(0,
                    ImmutableAttemptRequest.builder().from(ar1)
                    .timeZone(ZoneId.of("-0300"))
                    .build(),
                    wf1)
            );
        });

        // different retry attempt name doesn't conflict
        assertNotConflict(() -> {
            propagateOnly(ResourceConflictException.class, () ->
                exec.submitWorkflow(0,
                    ImmutableAttemptRequest.builder().from(ar1)
                    .retryAttemptName(Optional.of("retry1"))
                    .build(),
                    wf1)
            );
        });

        // different session time doesn't conflict
        assertNotConflict(() -> {
            propagateOnly(ResourceConflictException.class, () ->
                exec.submitWorkflow(0,
                    ImmutableAttemptRequest.builder().from(ar1)
                    .sessionTime(sessionTime1.plusSeconds(1))
                    .build(),
                    wf1)
            );
        });

        // different workflow name doesn't conflict
        assertNotConflict(() -> {
            propagateOnly(ResourceConflictException.class, () ->
                exec.submitWorkflow(0,
                    ImmutableAttemptRequest.builder().from(ar1)
                    .workflowName("another")
                    .build(),
                    wf1)
            );
        });

        // wrong siteId causes NotFound
        assertNotFound(() -> {
            propagateOnly(ResourceNotFoundException.class, () ->
                exec.submitWorkflow(1, ar1, wf1)
            );
        });

        // wrong project id causes NotFound
        assertNotFound(() -> {
            propagateOnly(ResourceNotFoundException.class, () ->
                exec.submitWorkflow(0,
                    ImmutableAttemptRequest.builder().from(ar1)
                    .stored(AttemptRequest.Stored.of(ImmutableStoredRevision.builder().from(rev).projectId(30).build(), wf1))
                    .retryAttemptName(Optional.of("retryWithWrongProjectId"))
                    .build(),
                    wf1)
            );
        });

        // wrong workflow id causes NotFound
        assertNotFound(() -> {
            propagateOnly(ResourceNotFoundException.class, () ->
                exec.submitWorkflow(0,
                    ImmutableAttemptRequest.builder().from(ar1)
                    .stored(AttemptRequest.Stored.of(rev, ImmutableStoredWorkflowDefinition.builder().from(wf1).id(30).build()))
                    .retryAttemptName(Optional.of("retryWithWrongWfId"))
                    .build(),
                    wf1)
            );
        });
    }

    @Test
    public void testGetAndNotFounds()
        throws Exception
    {
        Instant sessionTime1 = Instant.ofEpochSecond(Instant.now().getEpochSecond() / 3600 * 3600);
        Instant sessionTime2 = sessionTime1.plusSeconds(3600);

        WorkflowDefinition def1 = WorkflowDefinition.of(
                wf1.getName(),
                cf.create()
                    .setNested("+step1", cf.create().set("sh>", "echo step1"))
                    .setNested("+step2", cf.create().set("sh>", "echo step2")),
                ZoneId.of("UTC")
                );

        // session
        AttemptRequest ar1 = attemptBuilder.buildFromStoredWorkflow(
                rev,
                wf1,
                cf.create(),
                ScheduleTime.runNow(sessionTime1),
                Optional.absent());
        StoredSessionAttemptWithSession attempt1 = exec.submitWorkflow(0, ar1, def1);

        // session + different session time
        AttemptRequest ar2 = attemptBuilder.buildFromStoredWorkflow(
                rev,
                wf1,
                cf.create(),
                ScheduleTime.runNow(sessionTime2),
                Optional.absent());
        StoredSessionAttemptWithSession attempt2 = exec.submitWorkflow(0, ar2, def1);

        // session + different retry attempt name
        AttemptRequest ar3 = attemptBuilder.buildFromStoredWorkflow(
                rev,
                wf1,
                cf.create(),
                ScheduleTime.runNow(sessionTime2),
                Optional.of("attempt3"));
        StoredSessionAttemptWithSession attempt3 = exec.submitWorkflow(0, ar3, def1);

        SessionStore anotherSite = manager.getSessionStore(1);

        ////
        // manager internal getters
        //
        assertEquals(attempt1, manager.getAttemptWithSessionById(attempt1.getId()));
        assertEquals(attempt2, manager.getAttemptWithSessionById(attempt2.getId()));
        assertEquals(attempt3, manager.getAttemptWithSessionById(attempt3.getId()));
        assertNotFound(() -> manager.getAttemptWithSessionById(attempt3.getId() + 10));

        ////
        // public listings
        //
        assertEquals(ImmutableList.of(attempt3, attempt1),
                store.getSessions(false, 100, Optional.absent()));
        assertEquals(ImmutableList.of(attempt3),
                store.getSessions(false, 1, Optional.absent()));
        assertEquals(ImmutableList.of(attempt1),
                store.getSessions(false, 100, Optional.of(attempt3.getId())));
        assertEmpty(anotherSite.getSessions(false, 100, Optional.absent()));

        assertEquals(ImmutableList.of(attempt3, attempt2, attempt1),
                store.getSessions(true, 100, Optional.absent()));
        assertEquals(ImmutableList.of(attempt3, attempt2),
                store.getSessions(true, 2, Optional.absent()));
        assertEquals(ImmutableList.of(attempt2, attempt1),
                store.getSessions(true, 100, Optional.of(attempt3.getId())));
        assertEmpty(anotherSite.getSessions(true, 100, Optional.absent()));

        assertEquals(ImmutableList.of(attempt3, attempt1),
                store.getSessionsOfProject(false, proj.getId(), 100, Optional.absent()));
        assertEquals(ImmutableList.of(attempt3),
                store.getSessionsOfProject(false, proj.getId(), 1, Optional.absent()));
        assertEquals(ImmutableList.of(attempt1),
                store.getSessionsOfProject(false, proj.getId(), 100, Optional.of(attempt3.getId())));
        assertEmpty(anotherSite.getSessionsOfProject(false, proj.getId(), 100, Optional.absent()));
        // TODO test with another project

        assertEquals(ImmutableList.of(attempt3, attempt2, attempt1),
                store.getSessionsOfProject(true, proj.getId(), 100, Optional.absent()));
        assertEquals(ImmutableList.of(attempt3, attempt2),
                store.getSessionsOfProject(true, proj.getId(), 2, Optional.absent()));
        assertEquals(ImmutableList.of(attempt2, attempt1),
                store.getSessionsOfProject(true, proj.getId(), 100, Optional.of(attempt3.getId())));
        assertEmpty(anotherSite.getSessionsOfProject(true, proj.getId(), 100, Optional.absent()));
        // TODO test with another project

        assertEquals(ImmutableList.of(attempt3, attempt1),
                store.getSessionsOfWorkflow(false, wf1.getId(), 100, Optional.absent()));
        assertEquals(ImmutableList.of(attempt3),
                store.getSessionsOfWorkflow(false, wf1.getId(), 1, Optional.absent()));
        assertEquals(ImmutableList.of(attempt1),
                store.getSessionsOfWorkflow(false, wf1.getId(), 100, Optional.of(attempt3.getId())));
        assertEmpty(anotherSite.getSessionsOfWorkflow(false, wf1.getId(), 100, Optional.absent()));
        // TODO test with another workflow

        assertEquals(ImmutableList.of(attempt3, attempt2, attempt1),
                store.getSessionsOfWorkflow(true, wf1.getId(), 100, Optional.absent()));
        assertEquals(ImmutableList.of(attempt3, attempt2),
                store.getSessionsOfWorkflow(true, wf1.getId(), 2, Optional.absent()));
        assertEquals(ImmutableList.of(attempt2, attempt1),
                store.getSessionsOfWorkflow(true, wf1.getId(), 100, Optional.of(attempt3.getId())));
        assertEmpty(anotherSite.getSessionsOfWorkflow(true, wf1.getId(), 100, Optional.absent()));
        // TODO test with another workflow

        ////
        // public getters
        //
        assertEquals(attempt1, store.getSessionAttemptById(attempt1.getId()));
        assertEquals(attempt2, store.getSessionAttemptById(attempt2.getId()));
        assertNotFound(() ->store.getSessionAttemptById(attempt1.getId() + 10));
        assertNotFound(() -> anotherSite.getSessionAttemptById(attempt1.getId()));

        assertEquals(attempt1, store.getSessionAttemptByNames(proj.getId(), wf1.getName(), sessionTime1, ""));
        assertEquals(attempt2, store.getSessionAttemptByNames(proj.getId(), wf1.getName(), sessionTime2, ""));
        assertEquals(attempt3, store.getSessionAttemptByNames(proj.getId(), wf1.getName(), sessionTime2, "attempt3"));
        assertNotFound(() -> store.getSessionAttemptByNames(proj.getId() + 10, wf1.getName(), sessionTime1, ""));
        assertNotFound(() -> store.getSessionAttemptByNames(proj.getId(), wf1.getName() + " ", sessionTime1, ""));
        assertNotFound(() -> store.getSessionAttemptByNames(proj.getId(), wf1.getName(), sessionTime1.plusSeconds(10000), ""));
        assertNotFound(() -> store.getSessionAttemptByNames(proj.getId(), wf1.getName(), sessionTime1, " "));
        assertNotFound(() -> anotherSite.getSessionAttemptByNames(proj.getId(), wf1.getName(), sessionTime1, ""));
        assertNotFound(() -> anotherSite.getSessionAttemptByNames(proj.getId(), wf1.getName(), sessionTime2, ""));

        assertEquals(ImmutableList.of(attempt2, attempt3), store.getOtherAttempts(attempt2.getId()));
        assertEquals(ImmutableList.of(attempt2, attempt3), store.getOtherAttempts(attempt3.getId()));

        ////
        // task archving
        //
        List<ArchivedTask> activeArchive = store.getTasksOfAttempt(attempt1.getId());
        SessionAttemptSummary sum = manager.lockAttemptIfExists(
                attempt1.getId(),
                (store, summary) -> {
                    store.aggregateAndInsertTaskArchive(attempt1.getId());
                    return summary;
                }).get();
        assertEquals(activeArchive, store.getTasksOfAttempt(attempt1.getId()));
    }
}
