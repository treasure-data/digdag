package io.digdag.core.database;

import java.util.concurrent.atomic.AtomicReference;
import org.skife.jdbi.v2.IDBI;
import org.junit.*;
import io.digdag.core.repository.*;
import static io.digdag.core.database.DatabaseTestingUtils.*;
import static org.junit.Assert.*;

public class TestDatabaseRepositoryStoreManager
{
    private DbiProvider dbip;
    private RepositoryStoreManager manager;
    private RepositoryStore store;

    @Before
    public void setup()
    {
        dbip = setupDatabase();
        manager = new DatabaseRepositoryStoreManager(dbip.get(), createConfigMapper());
        store = manager.getRepositoryStore(0);
    }

    @After
    public void destroy()
    {
        dbip.close();
    }

    @Test
    public void testPutRepositoryRevisionWorkflow()
    {
        Repository repo = Repository.of("repo1");
        Revision rev = createRevision("rev1");
        WorkflowSource wf = createWorkflow("wf1");

        StoredRepository storedRepo = store.putRepository(
                repo,
                (lock) -> {
                    assertConflict(false, () -> {
                        StoredRevision storedRev = lock.putRevision(rev);
                        assertEquals(rev, Revision.revisionBuilder().from(storedRev).build());

                        StoredWorkflowSource storedWf = lock.insertWorkflow(storedRev.getId(), wf);
                        assertEquals(wf, WorkflowSource.workflowSourceBuilder().from(storedWf).build());
                    });
                    return lock.get();
                });
        assertEquals(repo, Repository.repositoryBuilder().from(storedRepo).build());
    }

    @Test
    public void testConflicts()
    {
        Repository repo = Repository.of("repo1");
        Revision rev = createRevision("rev1");
        WorkflowSource wf = createWorkflow("wf1");

        StoredRepository storedRepo = store.putRepository(
                repo,
                (lock) -> {
                    // revision overwrites
                    StoredRevision storedRev = lock.putRevision(rev);
                    StoredRevision storedRev2 = lock.putRevision(rev);
                    assertEquals(storedRev, storedRev2);

                    // workflow conflicts
                    assertConflict(false, () -> {
                        lock.insertWorkflow(storedRev.getId(), wf);
                    });
                    assertConflict(true, () -> {
                        lock.insertWorkflow(storedRev.getId(), wf);
                    });
                    return lock.get();
                });

        // repository overwrites
        StoredRepository storedRepo2 = store.putRepository(
                repo,
                (lock) -> lock.get());
        assertEquals(storedRepo, storedRepo2);
    }

    @Test
    public void testNotFounds()
        throws ResourceNotFoundException
    {
        Repository repo = Repository.of("repo1");
        Revision rev = createRevision("rev1");
        WorkflowSource wf = createWorkflow("wf1");

        AtomicReference<StoredRevision> revRef = new AtomicReference<>();
        AtomicReference<StoredWorkflowSource> wfRef = new AtomicReference<>();
        StoredRepository storedRepo = store.putRepository(
                repo,
                (lock) -> {
                    assertConflict(false, () -> {
                        revRef.set(lock.putRevision(rev));
                        wfRef.set(lock.insertWorkflow(revRef.get().getId(), wf));
                    });
                    return lock.get();
                });
        StoredRevision storedRev = revRef.get();
        StoredWorkflowSource storedWf = wfRef.get();

        assertNotNull(store.getRepositoryById(storedRepo.getId()));
        assertNotNull(store.getRepositoryByName(storedRepo.getName()));

        assertNotNull(store.getRevisionById(storedRev.getId()));
        assertNotNull(store.getRevisionByName(storedRepo.getId(), storedRev.getName()));

        assertNotNull(store.getWorkflowSourceById(storedWf.getId()));
        assertNotNull(store.getWorkflowSourceByName(storedRev.getId(), storedWf.getName()));

        assertNotFound(true, () -> store.getRepositoryById(storedRepo.getId() + 1));
        assertNotFound(true, () -> store.getRepositoryByName(storedRepo.getName() + " "));

        assertNotFound(true, () -> store.getRevisionById(storedRev.getId() + 1));
        assertNotFound(true, () -> store.getRevisionByName(storedRepo.getId() + 1, storedRev.getName()));
        assertNotFound(true, () -> store.getRevisionByName(storedRepo.getId(), storedRev.getName() + " "));

        assertNotFound(true, () -> store.getWorkflowSourceById(storedWf.getId() + 1));
        assertNotFound(true, () -> store.getWorkflowSourceByName(storedRev.getId() + 1, storedWf.getName()));
        assertNotFound(true, () -> store.getWorkflowSourceByName(storedRev.getId(), storedWf.getName() + " "));

        RepositoryStore another = manager.getRepositoryStore(1);

        assertNotFound(true, () -> another.getRepositoryById(storedRepo.getId()));
        assertNotFound(true, () -> another.getRepositoryByName(storedRepo.getName()));

        assertNotFound(true, () -> another.getRevisionById(storedRev.getId()));
        assertNotFound(true, () -> another.getRevisionByName(storedRepo.getId(), storedRev.getName()));

        assertNotFound(true, () -> another.getWorkflowSourceById(storedWf.getId()));
        assertNotFound(true, () -> another.getWorkflowSourceByName(storedRev.getId(), storedWf.getName()));
    }

    private static Revision createRevision(String name)
    {
        return Revision.revisionBuilder()
            .name(name)
            .globalParams(createConfig())
            .archiveType("none")
            .build();
    }

    private static WorkflowSource createWorkflow(String name)
    {
        return WorkflowSource.of(
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
