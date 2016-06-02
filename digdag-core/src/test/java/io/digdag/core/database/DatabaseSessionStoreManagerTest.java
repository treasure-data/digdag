package io.digdag.core.database;

import java.util.*;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.*;
import com.google.common.base.Optional;
import com.google.common.collect.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.core.session.*;
import io.digdag.core.workflow.*;
import io.digdag.spi.ScheduleTime;
import io.digdag.client.config.ConfigFactory;
import static io.digdag.core.database.DatabaseTestingUtils.*;
import static org.hamcrest.Matchers.is;
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
        WorkflowDefinition srcWf1 = createWorkflow("wf1");
        WorkflowDefinition srcWf2 = createWorkflow("wf2");

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
        StoredSessionWithLastAttempt session1 = store.getSessionById(attempt1.getSessionId());
        assertSessionAndLastAttemptEquals(session1, attempt1);
        assertEquals(ImmutableList.of(session1), store.getSessions(100, Optional.absent()));

        // session + different session time
        AttemptRequest ar2 = attemptBuilder.buildFromStoredWorkflow(
                rev,
                wf1,
                cf.create(),
                ScheduleTime.runNow(sessionTime2),
                Optional.absent());
        StoredSessionAttemptWithSession attempt2 = exec.submitWorkflow(0, ar2, def1);
        StoredSessionWithLastAttempt session2 = store.getSessionById(attempt2.getSessionId());
        assertSessionAndLastAttemptEquals(session2, attempt2);
        assertEquals(ImmutableList.of(session2, session1), store.getSessions(100, Optional.absent()));

        // session + different retry attempt name
        String retryAttemptName = "attempt3";
        AttemptRequest ar3 = attemptBuilder.buildFromStoredWorkflow(
                rev,
                wf1,
                cf.create(),
                ScheduleTime.runNow(sessionTime2),
                Optional.of(retryAttemptName));
        StoredSessionAttemptWithSession attempt3 = exec.submitWorkflow(0, ar3, def1);
        assertSessionAndLastAttemptEquals(session2, attempt2);
        StoredSessionWithLastAttempt session2AfterRetry = store.getSessionById(attempt2.getSessionId());
        assertThat(session2AfterRetry.getLastAttempt().getRetryAttemptName(), is(Optional.of(retryAttemptName)));
        assertEquals(ImmutableList.of(session2AfterRetry, session1), store.getSessions(100, Optional.absent()));

        SessionStore anotherSite = manager.getSessionStore(1);

        ////
        // manager internal getters
        //
        assertEquals(attempt1, manager.getAttemptWithSessionById(attempt1.getId()));
        assertEquals(attempt2, manager.getAttemptWithSessionById(attempt2.getId()));
        assertEquals(attempt3, manager.getAttemptWithSessionById(attempt3.getId()));
        assertNotFound(() -> manager.getAttemptWithSessionById(attempt3.getId() + 10));

        ////
        // public sessions listings
        //
        assertEquals(ImmutableList.of(session2AfterRetry, session1),
                store.getSessions(100, Optional.absent()));
        assertEquals(ImmutableList.of(session2AfterRetry),
                store.getSessions(1, Optional.absent()));
        assertEquals(ImmutableList.of(session1),
                store.getSessions(100, Optional.of(session2AfterRetry.getId())));
        assertEmpty(anotherSite.getSessions(100, Optional.absent()));


        ////
        // public attempt listings
        //
        assertEquals(ImmutableList.of(attempt3, attempt1),
                store.getAttempts(false, 100, Optional.absent()));
        assertEquals(ImmutableList.of(attempt3),
                store.getAttempts(false, 1, Optional.absent()));
        assertEquals(ImmutableList.of(attempt1),
                store.getAttempts(false, 100, Optional.of(attempt3.getId())));
        assertEmpty(anotherSite.getAttempts(false, 100, Optional.absent()));

        assertEquals(ImmutableList.of(attempt3, attempt2, attempt1),
                store.getAttempts(true, 100, Optional.absent()));
        assertEquals(ImmutableList.of(attempt3, attempt2),
                store.getAttempts(true, 2, Optional.absent()));
        assertEquals(ImmutableList.of(attempt2, attempt1),
                store.getAttempts(true, 100, Optional.of(attempt3.getId())));
        assertEmpty(anotherSite.getAttempts(true, 100, Optional.absent()));

        assertEquals(ImmutableList.of(attempt3, attempt1),
                store.getAttemptsOfProject(false, proj.getId(), 100, Optional.absent()));
        assertEquals(ImmutableList.of(attempt3),
                store.getAttemptsOfProject(false, proj.getId(), 1, Optional.absent()));
        assertEquals(ImmutableList.of(attempt1),
                store.getAttemptsOfProject(false, proj.getId(), 100, Optional.of(attempt3.getId())));
        assertEmpty(anotherSite.getAttemptsOfProject(false, proj.getId(), 100, Optional.absent()));
        // TODO test with another project

        assertEquals(ImmutableList.of(attempt3, attempt2, attempt1),
                store.getAttemptsOfProject(true, proj.getId(), 100, Optional.absent()));
        assertEquals(ImmutableList.of(attempt3, attempt2),
                store.getAttemptsOfProject(true, proj.getId(), 2, Optional.absent()));
        assertEquals(ImmutableList.of(attempt2, attempt1),
                store.getAttemptsOfProject(true, proj.getId(), 100, Optional.of(attempt3.getId())));
        assertEmpty(anotherSite.getAttemptsOfProject(true, proj.getId(), 100, Optional.absent()));
        // TODO test with another project

        assertEquals(ImmutableList.of(attempt3, attempt1),
                store.getAttemptsOfWorkflow(false, wf1.getId(), 100, Optional.absent()));
        assertEquals(ImmutableList.of(attempt3),
                store.getAttemptsOfWorkflow(false, wf1.getId(), 1, Optional.absent()));
        assertEquals(ImmutableList.of(attempt1),
                store.getAttemptsOfWorkflow(false, wf1.getId(), 100, Optional.of(attempt3.getId())));
        assertEmpty(anotherSite.getAttemptsOfWorkflow(false, wf1.getId(), 100, Optional.absent()));
        // TODO test with another workflow

        assertEquals(ImmutableList.of(attempt3, attempt2, attempt1),
                store.getAttemptsOfWorkflow(true, wf1.getId(), 100, Optional.absent()));
        assertEquals(ImmutableList.of(attempt3, attempt2),
                store.getAttemptsOfWorkflow(true, wf1.getId(), 2, Optional.absent()));
        assertEquals(ImmutableList.of(attempt2, attempt1),
                store.getAttemptsOfWorkflow(true, wf1.getId(), 100, Optional.of(attempt3.getId())));
        assertEmpty(anotherSite.getAttemptsOfWorkflow(true, wf1.getId(), 100, Optional.absent()));
        // TODO test with another workflow

        StoredSessionAttempt rawAttempt1 = StoredSessionAttempt.copyOf(attempt1);
        StoredSessionAttempt rawAttempt2 = StoredSessionAttempt.copyOf(attempt2);
        StoredSessionAttempt rawAttempt3 = StoredSessionAttempt.copyOf(attempt3);

        assertEquals(ImmutableList.of(rawAttempt1),
                store.getAttemptsOfSession(session1.getId(), 100, Optional.absent()));
        assertEquals(ImmutableList.of(rawAttempt1),
                store.getAttemptsOfSession(session1.getId(), 1, Optional.absent()));
        assertEmpty(store.getAttemptsOfSession(session1.getId(), 100, Optional.of(attempt1.getId())));
        assertEmpty(anotherSite.getAttemptsOfSession(session1.getId(), 100, Optional.absent()));
        // TODO test with another workflow

        assertEquals(ImmutableList.of(rawAttempt3, rawAttempt2),
                store.getAttemptsOfSession(session2.getId(), 100, Optional.absent()));
        assertEquals(ImmutableList.of(rawAttempt3),
                store.getAttemptsOfSession(session2.getId(), 1, Optional.absent()));
        assertEquals(ImmutableList.of(rawAttempt2),
                store.getAttemptsOfSession(session2.getId(), 100, Optional.of(rawAttempt3.getId())));
        assertEmpty(anotherSite.getAttemptsOfSession(session2.getId(), 100, Optional.absent()));
        // TODO test with another workflow

        ////
        // public getters
        //
        assertEquals(attempt1, store.getAttemptById(attempt1.getId()));
        assertEquals(attempt2, store.getAttemptById(attempt2.getId()));
        assertNotFound(() ->store.getAttemptById(attempt1.getId() + 10));
        assertNotFound(() -> anotherSite.getAttemptById(attempt1.getId()));

        assertEquals(attempt1, store.getAttemptByName(proj.getId(), wf1.getName(), sessionTime1, ""));
        assertEquals(attempt2, store.getAttemptByName(proj.getId(), wf1.getName(), sessionTime2, ""));
        assertEquals(attempt3, store.getAttemptByName(proj.getId(), wf1.getName(), sessionTime2, retryAttemptName));
        assertNotFound(() -> store.getAttemptByName(proj.getId() + 10, wf1.getName(), sessionTime1, ""));
        assertNotFound(() -> store.getAttemptByName(proj.getId(), wf1.getName() + " ", sessionTime1, ""));
        assertNotFound(() -> store.getAttemptByName(proj.getId(), wf1.getName(), sessionTime1.plusSeconds(10000), ""));
        assertNotFound(() -> store.getAttemptByName(proj.getId(), wf1.getName(), sessionTime1, " "));
        assertNotFound(() -> anotherSite.getAttemptByName(proj.getId(), wf1.getName(), sessionTime1, ""));
        assertNotFound(() -> anotherSite.getAttemptByName(proj.getId(), wf1.getName(), sessionTime2, ""));

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

    private void assertSessionAndLastAttemptEquals(StoredSessionWithLastAttempt session, StoredSessionAttemptWithSession attempt)
    {
        assertThat(session.getId(), is(attempt.getSessionId()));
        assertThat(session.getUuid(), is(attempt.getSessionUuid()));
        assertThat(session.getLastAttemptId(), is(attempt.getId()));
        assertThat(session.getLastAttempt(), is(StoredSessionAttempt.copyOf(attempt)));

    }
}
