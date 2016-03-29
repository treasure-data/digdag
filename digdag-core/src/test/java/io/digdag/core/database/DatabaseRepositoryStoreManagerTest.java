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
import static java.nio.charset.StandardCharsets.UTF_8;
import static io.digdag.core.database.DatabaseTestingUtils.*;
import static org.junit.Assert.*;

public class DatabaseRepositoryStoreManagerTest
{
    private DatabaseFactory factory;
    private RepositoryStoreManager manager;
    private SchedulerManager sm;
    private RepositoryStore store;

    @Before
    public void setUp()
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
    public void testConflicts()
        throws Exception
    {
        Repository srcRepo1 = Repository.of("repo1");
        Revision srcRev1 = createRevision("rev1");
        WorkflowDefinition srcWf1 = createWorkflow("+wf1");

        StoredRepository storedRepo = store.putAndLockRepository(
                srcRepo1,
                (store, stored) -> stored);

        // putAndLockRepository doesn't conflict
        StoredRepository storedRepo2 = store.putAndLockRepository(
                srcRepo1,
                (store, stored) -> stored);

        assertEquals(storedRepo, storedRepo2);

        StoredRevision storedRev = store.putAndLockRepository(
                srcRepo1,
                (store, stored) -> {
                    RepositoryControl lock = new RepositoryControl(store, stored);
                    return lock.insertRevision(srcRev1);
                });

        assertConflict(() -> {
            store.putAndLockRepository(
                    srcRepo1,
                    (store, stored) -> {
                        RepositoryControl lock = new RepositoryControl(store, stored);

                        // workflow conflicts if name conflicts
                        assertNotConflict(() -> {
                            lock.insertWorkflowDefinitions(storedRev, ImmutableList.of(srcWf1), sm, Instant.now());
                        });
                        return lock.insertWorkflowDefinitions(storedRev, ImmutableList.of(srcWf1), sm, Instant.now());
                    });
        });

        assertConflict(() -> {
            store.putAndLockRepository(
                    srcRepo1,
                    (store, stored) -> {
                        RepositoryControl lock = new RepositoryControl(store, stored);

                        // revision conflicts if name conflicts
                        return lock.insertRevision(srcRev1);
                    });
        });
    }

    @Test
    public void testGetAndNotFounds()
        throws Exception
    {
        Repository srcRepo1 = Repository.of("repo1");
        Revision srcRev1 = createRevision("rev1");
        WorkflowDefinition srcWf1 = createWorkflow("+wf1");

        Repository srcRepo2 = Repository.of("repo2");
        Revision srcRev2 = createRevision("rev2");
        WorkflowDefinition srcWf2 = createWorkflow("+wf2");

        Revision srcRev3 = createRevision("rev3");
        WorkflowDefinition srcWf3 = createWorkflow("+wf3");
        WorkflowDefinition srcWf4 = createWorkflow("+wf4");

        final AtomicReference<StoredRevision> revRef = new AtomicReference<>();
        final AtomicReference<StoredWorkflowDefinition> wfRefA = new AtomicReference<>();
        final AtomicReference<StoredWorkflowDefinition> wfRefB = new AtomicReference<>();

        StoredRepository repo1 = store.putAndLockRepository(
                srcRepo1,
                (store, stored) -> {
                    RepositoryControl lock = new RepositoryControl(store, stored);
                    assertNotConflict(() -> {
                        revRef.set(lock.insertRevision(srcRev1));
                        wfRefA.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(srcWf1), sm, Instant.now()).get(0));
                    });
                    return lock.get();
                });
        StoredRevision rev1 = revRef.get();
        StoredWorkflowDefinition wf1 = wfRefA.get();
        StoredWorkflowDefinitionWithRepository wfDetails1 = StoredWorkflowDefinitionWithRepository.of(wf1, repo1, srcRev1);

        StoredRepository repo2 = store.putAndLockRepository(
                srcRepo2,
                (store, stored) -> {
                    RepositoryControl lock = new RepositoryControl(store, stored);
                    assertNotConflict(() -> {
                        revRef.set(lock.insertRevision(srcRev2));
                        wfRefA.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(srcWf2), sm, Instant.now()).get(0));
                    });
                    return lock.get();
                });
        StoredRevision rev2 = revRef.get();
        StoredWorkflowDefinition wf2 = wfRefA.get();
        StoredWorkflowDefinitionWithRepository wfDetails2 = StoredWorkflowDefinitionWithRepository.of(wf2, repo2, srcRev2);

        store.putAndLockRepository(
                srcRepo2,
                (store, stored) -> {
                    RepositoryControl lock = new RepositoryControl(store, stored);
                    assertNotConflict(() -> {
                        revRef.set(lock.insertRevision(srcRev3));
                        wfRefA.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(srcWf3), sm, Instant.now()).get(0));
                        wfRefB.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(srcWf4), sm, Instant.now()).get(0));
                    });
                    return lock.get();
                });
        StoredRevision rev3 = revRef.get();
        StoredWorkflowDefinition wf3 = wfRefA.get();
        StoredWorkflowDefinition wf4 = wfRefB.get();
        StoredWorkflowDefinitionWithRepository wfDetails3 = StoredWorkflowDefinitionWithRepository.of(wf3, repo2, srcRev3);

        RepositoryStore anotherSite = manager.getRepositoryStore(1);

        ////
        // return value of setters
        //
        assertEquals(srcRepo1, ImmutableRepository.builder().from(repo1).build());
        assertEquals(srcRev1, ImmutableRevision.builder().from(rev1).build());
        assertEquals(srcWf1, ImmutableWorkflowDefinition.builder().from(wf1).build());

        assertEquals(srcRepo1, ImmutableRepository.builder().from(repo1).build());
        assertEquals(srcRev1, ImmutableRevision.builder().from(rev1).build());
        assertEquals(srcWf1, ImmutableWorkflowDefinition.builder().from(wf1).build());

        ////
        // manager internal getters
        //
        assertEquals(repo1, manager.getRepositoryByIdInternal(repo1.getId()));
        assertEquals(repo2, manager.getRepositoryByIdInternal(repo2.getId()));
        assertNotFound(() -> manager.getRepositoryByIdInternal(repo1.getId() + 10));

        assertEquals(wfDetails1, manager.getWorkflowDetailsById(wf1.getId()));
        assertEquals(wfDetails2, manager.getWorkflowDetailsById(wf2.getId()));
        assertNotFound(() -> manager.getWorkflowDetailsById(wf1.getId() + 10));

        assertEquals(rev1, manager.getRevisionOfWorkflowDefinition(wf1.getId()));
        assertEquals(rev2, manager.getRevisionOfWorkflowDefinition(wf2.getId()));
        assertNotFound(() -> manager.getRevisionOfWorkflowDefinition(wf1.getId() + 10));

        ////
        // public simple listings
        //
        assertEquals(ImmutableList.of(repo1, repo2), store.getRepositories(100, Optional.absent()));
        assertEquals(ImmutableList.of(repo1), store.getRepositories(1, Optional.absent()));
        assertEquals(ImmutableList.of(repo2), store.getRepositories(100, Optional.of(repo1.getId())));
        assertEmpty(anotherSite.getRepositories(100, Optional.absent()));

        assertEquals(ImmutableList.of(rev3, rev2), store.getRevisions(repo2.getId(), 100, Optional.absent()));  // revision is returned in reverse order
        assertEquals(ImmutableList.of(rev3), store.getRevisions(repo2.getId(), 1, Optional.absent()));
        assertEquals(ImmutableList.of(rev2), store.getRevisions(repo2.getId(), 100, Optional.of(rev3.getId())));
        assertEmpty(anotherSite.getRevisions(repo2.getId(), 100, Optional.absent()));

        assertEquals(ImmutableList.of(wf3, wf4), store.getWorkflowDefinitions(rev3.getId(), 100, Optional.absent()));
        assertEquals(ImmutableList.of(wf3), store.getWorkflowDefinitions(rev3.getId(), 1, Optional.absent()));
        assertEquals(ImmutableList.of(wf4), store.getWorkflowDefinitions(rev3.getId(), 100, Optional.of(wf3.getId())));
        assertEmpty(anotherSite.getWorkflowDefinitions(rev3.getId(), 100, Optional.absent()));

        ////
        // public simple getters
        //
        assertEquals(repo1, store.getRepositoryById(repo1.getId()));
        assertEquals(repo2, store.getRepositoryById(repo2.getId()));
        assertNotFound(() -> store.getRepositoryById(repo1.getId() + 10));
        assertNotFound(() -> anotherSite.getRepositoryById(repo1.getId()));

        assertEquals(repo1, store.getRepositoryByName(repo1.getName()));
        assertEquals(repo2, store.getRepositoryByName(repo2.getName()));
        assertNotFound(() -> store.getRepositoryByName(repo1.getName() + " "));
        assertNotFound(() -> anotherSite.getRepositoryByName(repo1.getName()));

        assertEquals(rev1, store.getRevisionById(rev1.getId()));
        assertEquals(rev2, store.getRevisionById(rev2.getId()));
        assertNotFound(() -> store.getRevisionById(rev1.getId() + 10));
        assertNotFound(() -> anotherSite.getRevisionById(rev1.getId()));

        assertEquals(rev1, store.getRevisionByName(repo1.getId(), rev1.getName()));
        assertEquals(rev2, store.getRevisionByName(repo2.getId(), rev2.getName()));
        assertNotFound(() -> store.getRevisionByName(repo1.getId() + 10, rev1.getName()));
        assertNotFound(() -> store.getRevisionByName(repo1.getId(), rev2.getName()));
        assertNotFound(() -> anotherSite.getRevisionByName(repo1.getId(), rev1.getName()));

        assertEquals(wfDetails1, store.getWorkflowDefinitionById(wf1.getId()));
        assertEquals(wfDetails2, store.getWorkflowDefinitionById(wf2.getId()));
        assertNotFound(() -> store.getWorkflowDefinitionById(wf1.getId() + 10));
        assertNotFound(() -> anotherSite.getWorkflowDefinitionById(wf1.getId()));

        assertEquals(wf1, store.getWorkflowDefinitionByName(rev1.getId(), wf1.getName()));
        assertEquals(wf2, store.getWorkflowDefinitionByName(rev2.getId(), wf2.getName()));
        assertNotFound(() -> store.getWorkflowDefinitionByName(rev1.getId() + 10, wf1.getName()));
        assertNotFound(() -> store.getWorkflowDefinitionByName(rev1.getId(), wf2.getName()));
        assertNotFound(() -> anotherSite.getWorkflowDefinitionByName(rev1.getId(), wf1.getName()));

        ////
        // complex getters
        //
        assertEquals(rev1, store.getLatestRevision(repo1.getId()));
        assertEquals(rev3, store.getLatestRevision(repo2.getId()));
        assertNotFound(() -> anotherSite.getLatestRevision(repo1.getId()));

        assertEquals(wfDetails3, store.getLatestWorkflowDefinitionByName(repo2.getId(), wf3.getName()));
        assertNotFound(() -> store.getLatestWorkflowDefinitionByName(repo2.getId(), wf2.getName()));

        // getRevisionArchiveData returns NotFound if insertRevisionArchiveData is not called
        assertNotFound(() -> store.getRevisionArchiveData(rev1.getId()));
    }

    @Test
    public void testRevisionArchiveData()
        throws Exception
    {
        byte[] data = "archive data".getBytes(UTF_8);

        StoredRevision rev = store.putAndLockRepository(
                Repository.of("repo1"),
                (store, stored) -> {
                    RepositoryControl lock = new RepositoryControl(store, stored);

                    StoredRevision storedRev = lock.insertRevision(createRevision("rev1"));
                    lock.insertRevisionArchiveData(storedRev.getId(), data);

                    return storedRev;
                });

        assertArrayEquals(data, store.getRevisionArchiveData(rev.getId()));
        assertNotFound(() -> store.getRevisionArchiveData(rev.getId() + 10));
    }
}
