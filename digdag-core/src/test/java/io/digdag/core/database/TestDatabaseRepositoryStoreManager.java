package io.digdag.core.database;

import java.util.*;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import org.skife.jdbi.v2.IDBI;
import org.junit.*;
import com.google.common.collect.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import static io.digdag.core.database.DatabaseTestingUtils.*;
import static org.junit.Assert.*;

public class TestDatabaseRepositoryStoreManager
{
    private DatabaseFactory factory;
    private RepositoryStoreManager manager;
    private SchedulerManager sm;
    private RepositoryStore store;

    @Before
    public void setup()
    {
        factory = setupDatabase();
        manager = factory.getRepositoryStoreManager();
        sm = new SchedulerManager(ImmutableSet.of());
        store = manager.getRepositoryStore(0);
    }

    @After
    public void destroy()
    {
        factory.close();
    }

    @Test
    public void testPutRepositoryRevisionWorkflow()
        throws Exception
    {
        Repository repo = Repository.of("repo1");
        Revision rev = createRevision("rev1");
        WorkflowDefinition wf = createWorkflow("+wf1");

        StoredRepository storedRepo = store.putAndLockRepository(
                repo,
                (store, stored) -> {
                    RepositoryControl lock = new RepositoryControl(store, stored);
                    assertConflict(false, () -> {
                        StoredRevision storedRev = lock.insertRevision(rev);
                        assertEquals(rev, Revision.copyOf(storedRev));

                        List<StoredWorkflowDefinition> storedWfs = lock.insertWorkflowDefinitions(storedRev, ImmutableList.of(wf), sm, Instant.now());
                        assertEquals(wf, WorkflowDefinition.workflowSourceBuilder().from(storedWfs.get(0)).build());
                    });
                    return lock.get();
                });
        assertEquals(repo, Repository.repositoryBuilder().from(storedRepo).build());
    }

    @Test
    public void testConflicts()
        throws Exception
    {
        Repository repo = Repository.of("repo1");
        Revision rev = createRevision("rev1");
        WorkflowDefinition wf = createWorkflow("+wf1");

        StoredRepository storedRepo = store.putAndLockRepository(
                repo,
                (store, stored) -> {
                    RepositoryControl lock = new RepositoryControl(store, stored);

                    StoredRevision storedRev = lock.insertRevision(rev);

                    // workflow conflicts
                    assertConflict(false, () -> {
                        lock.insertWorkflowDefinitions(storedRev, ImmutableList.of(wf), sm, Instant.now());
                    });
                    assertConflict(true, () -> {
                        lock.insertWorkflowDefinitions(storedRev, ImmutableList.of(wf), sm, Instant.now());
                    });
                    return lock.get();
                });

        // repository overwrites
        StoredRepository storedRepo2 = store.putAndLockRepository(
                repo,
                (store, stored) -> {
                    RepositoryControl lock = new RepositoryControl(store, stored);

                    // revision conflicts
                    assertConflict(true, () -> {
                        lock.insertRevision(rev);
                    });

                    return lock.get();
                });
        assertEquals(storedRepo, storedRepo2);
    }

    @Test
    public void testNotFounds()
        throws Exception
    {
        Repository repo = Repository.of("repo1");
        Revision rev = createRevision("rev1");
        WorkflowDefinition wf = createWorkflow("+wf1");

        AtomicReference<StoredRevision> revRef = new AtomicReference<>();
        AtomicReference<StoredWorkflowDefinition> wfRef = new AtomicReference<>();
        StoredRepository storedRepo = store.putAndLockRepository(
                repo,
                (store, stored) -> {
                    RepositoryControl lock = new RepositoryControl(store, stored);
                    assertConflict(false, () -> {
                        revRef.set(lock.insertRevision(rev));
                        wfRef.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(wf), sm, Instant.now()).get(0));
                    });
                    return lock.get();
                });
        StoredRevision storedRev = revRef.get();
        StoredWorkflowDefinition storedWf = wfRef.get();

        assertNotNull(store.getRepositoryById(storedRepo.getId()));
        assertNotNull(store.getRepositoryByName(storedRepo.getName()));

        assertNotNull(store.getRevisionById(storedRev.getId()));
        assertNotNull(store.getRevisionByName(storedRepo.getId(), storedRev.getName()));

        assertNotNull(store.getWorkflowDefinitionById(storedWf.getId()));
        assertNotNull(store.getWorkflowDefinitionByName(storedRev.getId(), storedWf.getName()));

        assertNotFound(true, () -> store.getRepositoryById(storedRepo.getId() + 1));
        assertNotFound(true, () -> store.getRepositoryByName(storedRepo.getName() + " "));

        assertNotFound(true, () -> store.getRevisionById(storedRev.getId() + 1));
        assertNotFound(true, () -> store.getRevisionByName(storedRepo.getId() + 1, storedRev.getName()));
        assertNotFound(true, () -> store.getRevisionByName(storedRepo.getId(), storedRev.getName() + " "));

        assertNotFound(true, () -> store.getWorkflowDefinitionById(storedWf.getId() + 1));
        assertNotFound(true, () -> store.getWorkflowDefinitionByName(storedRev.getId() + 1, storedWf.getName()));
        assertNotFound(true, () -> store.getWorkflowDefinitionByName(storedRev.getId(), storedWf.getName() + " "));

        RepositoryStore another = manager.getRepositoryStore(1);

        assertNotFound(true, () -> another.getRepositoryById(storedRepo.getId()));
        assertNotFound(true, () -> another.getRepositoryByName(storedRepo.getName()));

        assertNotFound(true, () -> another.getRevisionById(storedRev.getId()));
        assertNotFound(true, () -> another.getRevisionByName(storedRepo.getId(), storedRev.getName()));

        assertNotFound(true, () -> another.getWorkflowDefinitionById(storedWf.getId()));
        assertNotFound(true, () -> another.getWorkflowDefinitionByName(storedRev.getId(), storedWf.getName()));
    }

    private static Revision createRevision(String name)
    {
        return ImmutableRevision.builder()
            .name(name)
            .defaultParams(createConfig())
            .archiveType("none")
            .build();
    }

    private static WorkflowDefinition createWorkflow(String name)
    {
        return WorkflowDefinition.of(
                name,
                createConfig().set("uniq", System.nanoTime()));
    }

    private interface MayConflict
    {
        void run() throws ResourceConflictException;
    }

    private interface MayNotFound
    {
        void run() throws ResourceNotFoundException;
    }

    private static void assertNotFound(boolean cause, MayNotFound r)
    {
        try {
            r.run();
            if (cause) {
                fail();
            }
        }
        catch (ResourceNotFoundException ex) {
            if (!cause) {
                fail();
            }
        }
    }

    private static void assertConflict(boolean cause, MayConflict r)
    {
        try {
            r.run();
            if (cause) {
                fail();
            }
        }
        catch (ResourceConflictException ex) {
            if (!cause) {
                fail();
            }
        }
    }
}
