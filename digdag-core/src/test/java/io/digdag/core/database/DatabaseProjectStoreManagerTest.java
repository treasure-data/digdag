package io.digdag.core.database;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.digdag.core.repository.ImmutableProject;
import io.digdag.core.repository.ImmutableRevision;
import io.digdag.core.repository.ImmutableWorkflowDefinition;
import io.digdag.core.repository.Project;
import io.digdag.core.repository.ProjectControl;
import io.digdag.core.repository.ProjectMap;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.Revision;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredProjectWithRevision;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.core.repository.TimeZoneMap;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.schedule.SchedulerManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.digdag.core.database.DatabaseTestingUtils.assertConflict;
import static io.digdag.core.database.DatabaseTestingUtils.assertEmpty;
import static io.digdag.core.database.DatabaseTestingUtils.assertNotConflict;
import static io.digdag.core.database.DatabaseTestingUtils.assertNotFound;
import static io.digdag.core.database.DatabaseTestingUtils.createRevision;
import static io.digdag.core.database.DatabaseTestingUtils.createWorkflow;
import static io.digdag.core.database.DatabaseTestingUtils.setupDatabase;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class DatabaseProjectStoreManagerTest
{
    private DatabaseFactory factory;
    private ProjectStoreManager manager;
    private SchedulerManager sm;
    private ProjectStore store;

    @Before
    public void setUp()
            throws Exception
    {
        factory = setupDatabase();
        factory.begin(() -> {
            manager = factory.getProjectStoreManager();
            sm = new SchedulerManager(ImmutableSet.of());
            store = manager.getProjectStore(0);
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
        Project srcProj1 = Project.of("proj1");
        Revision srcRev1 = createRevision("rev1");
        WorkflowDefinition srcWf1 = createWorkflow("wf1");

        StoredRevision storedRev = factory.begin(() -> {
            StoredProject storedProj = store.putAndLockProject(
                    srcProj1,
                    (store, stored) -> stored);

            // putAndLockProject doesn't conflict
            StoredProject storedProj2 = store.putAndLockProject(
                    srcProj1,
                    (store, stored) -> stored);

            assertEquals(storedProj, storedProj2);

            return store.putAndLockProject(
                    srcProj1,
                    (store, stored) -> {
                        ProjectControl lock = new ProjectControl(store, stored);
                        return lock.insertRevision(srcRev1);
                    });
        });

        factory.begin(() -> {
            assertConflict(() -> {
                store.putAndLockProject(
                        srcProj1,
                        (store, stored) -> {
                            ProjectControl lock = new ProjectControl(store, stored);

                            // workflow conflicts if name conflicts
                            assertNotConflict(() -> {
                                lock.insertWorkflowDefinitions(storedRev, ImmutableList.of(srcWf1), sm, Instant.now());
                            });
                            return lock.insertWorkflowDefinitions(storedRev, ImmutableList.of(srcWf1), sm, Instant.now());
                        });
            });
        });

        factory.begin(() -> {
            assertConflict(() -> {
                store.putAndLockProject(
                        srcProj1,
                        (store, stored) -> {
                            ProjectControl lock = new ProjectControl(store, stored);

                            // revision conflicts if name conflicts
                            return lock.insertRevision(srcRev1);
                        });
            });
        });
    }

    @Test
    public void testGetAndNotFounds()
        throws Exception
    {
        factory.begin(() -> {
            Project srcProj1 = Project.of("proj1");
            Revision srcRev1 = createRevision("rev1");
            WorkflowDefinition srcWf1 = createWorkflow("wf1");

            Project srcProj2 = Project.of("proj2");
            Revision srcRev2 = createRevision("rev2");
            WorkflowDefinition srcWf2 = createWorkflow("wf2");

            Revision srcRev3 = createRevision("rev3");
            WorkflowDefinition srcWf3 = createWorkflow("wf3");
            WorkflowDefinition srcWf4 = createWorkflow("wf4");

            final AtomicReference<StoredRevision> revRef = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRefA = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRefB = new AtomicReference<>();

            StoredProject proj1 = store.putAndLockProject(
                    srcProj1,
                    (store, stored) -> {
                        ProjectControl lock = new ProjectControl(store, stored);
                        assertNotConflict(() -> {
                            revRef.set(lock.insertRevision(srcRev1));
                            wfRefA.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(srcWf1), sm, Instant.now()).get(0));
                        });
                        return lock.get();
                    });
            StoredRevision rev1 = revRef.get();
            StoredWorkflowDefinition wf1 = wfRefA.get();
            StoredWorkflowDefinitionWithProject wfDetails1 = StoredWorkflowDefinitionWithProject.of(wf1, proj1, srcRev1);

            StoredProject proj2 = store.putAndLockProject(
                    srcProj2,
                    (store, stored) -> {
                        ProjectControl lock = new ProjectControl(store, stored);
                        assertNotConflict(() -> {
                            revRef.set(lock.insertRevision(srcRev2));
                            wfRefA.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(srcWf2), sm, Instant.now()).get(0));
                        });
                        return lock.get();
                    });
            StoredRevision rev2 = revRef.get();
            StoredWorkflowDefinition wf2 = wfRefA.get();
            StoredWorkflowDefinitionWithProject wfDetails2 = StoredWorkflowDefinitionWithProject.of(wf2, proj2, srcRev2);

            store.putAndLockProject(
                    srcProj2,
                    (store, stored) -> {
                        ProjectControl lock = new ProjectControl(store, stored);
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
            StoredWorkflowDefinitionWithProject wfDetails3 = StoredWorkflowDefinitionWithProject.of(wf3, proj2, srcRev3);
            StoredWorkflowDefinitionWithProject wfDetails4 = StoredWorkflowDefinitionWithProject.of(wf4, proj2, srcRev3);

            StoredProjectWithRevision proj1Rev1 = StoredProjectWithRevision.of(proj1, rev1);
            StoredProjectWithRevision proj2Rev3 = StoredProjectWithRevision.of(proj2, rev3);

            ProjectStore anotherSite = manager.getProjectStore(1);

            ////
            // return value of setters
            //
            assertEquals(srcProj1, ImmutableProject.builder().from(proj1).build());
            assertEquals(srcRev1, ImmutableRevision.builder().from(rev1).build());
            assertEquals(srcWf1, ImmutableWorkflowDefinition.builder().from(wf1).build());

            assertEquals(srcProj1, ImmutableProject.builder().from(proj1).build());
            assertEquals(srcRev1, ImmutableRevision.builder().from(rev1).build());
            assertEquals(srcWf1, ImmutableWorkflowDefinition.builder().from(wf1).build());

            ////
            // manager internal getters
            //
            assertEquals(proj1, manager.getProjectByIdInternal(proj1.getId()));
            assertEquals(proj2, manager.getProjectByIdInternal(proj2.getId()));
            assertNotFound(() -> manager.getProjectByIdInternal(proj1.getId() + 10));
            assertFalse(proj1.getDeletedAt().isPresent());

            assertEquals(wfDetails1, manager.getWorkflowDetailsById(wf1.getId()));
            assertEquals(wfDetails2, manager.getWorkflowDetailsById(wf2.getId()));
            assertNotFound(() -> manager.getWorkflowDetailsById(wf1.getId() + 10));

            assertEquals(rev1, manager.getRevisionOfWorkflowDefinition(wf1.getId()));
            assertEquals(rev2, manager.getRevisionOfWorkflowDefinition(wf2.getId()));
            assertNotFound(() -> manager.getRevisionOfWorkflowDefinition(wf1.getId() + 10));

            ////
            // public simple listings
            //
            assertEquals(ImmutableList.of(proj1, proj2), store.getProjects(100, Optional.absent(), Optional.absent(), () -> "true"));
            assertEquals(ImmutableList.of(proj1), store.getProjects(1, Optional.absent(), Optional.absent(), () -> "true"));
            assertEquals(ImmutableList.of(proj2), store.getProjects(100, Optional.of(proj1.getId()), Optional.absent(), () -> "true"));
            assertEmpty(anotherSite.getProjects(100, Optional.absent(), Optional.absent(), () -> "true"));

            assertEquals(ImmutableList.of(proj1Rev1, proj2Rev3), store.getProjectsWithLatestRevision(100, Optional.absent(), Optional.absent(), () -> "true"));
            assertEquals(ImmutableList.of(proj1Rev1), store.getProjectsWithLatestRevision(1, Optional.absent(), Optional.absent(), () -> "true"));
            assertEquals(ImmutableList.of(proj2Rev3), store.getProjectsWithLatestRevision(100, Optional.of(proj1.getId()), Optional.absent(), () -> "true"));
            assertEmpty(anotherSite.getProjectsWithLatestRevision(100, Optional.absent(), Optional.absent(), () -> "true"));

            assertEquals(ImmutableList.of(rev3, rev2), store.getRevisions(proj2.getId(), 100, Optional.absent()));  // revision is returned in reverse order
            assertEquals(ImmutableList.of(rev3), store.getRevisions(proj2.getId(), 1, Optional.absent()));
            assertEquals(ImmutableList.of(rev2), store.getRevisions(proj2.getId(), 100, Optional.of(rev3.getId())));
            assertEmpty(anotherSite.getRevisions(proj2.getId(), 100, Optional.absent()));

            assertEquals(ImmutableList.of(wf3, wf4), store.getWorkflowDefinitions(rev3.getId(), 100, Optional.absent(), () -> "true"));
            assertEquals(ImmutableList.of(wf3), store.getWorkflowDefinitions(rev3.getId(), 1, Optional.absent(), () -> "true"));
            assertEquals(ImmutableList.of(wf4), store.getWorkflowDefinitions(rev3.getId(), 100, Optional.of(wf3.getId()), () -> "true"));
            assertEmpty(anotherSite.getWorkflowDefinitions(rev3.getId(), 100, Optional.absent(), () -> "true"));

            assertEquals(ImmutableList.of(wfDetails1, wfDetails3, wfDetails4), store.getLatestActiveWorkflowDefinitions(100, Optional.absent(), Optional.absent(), () -> "true"));
            assertEquals(ImmutableList.of(wfDetails1), store.getLatestActiveWorkflowDefinitions(1, Optional.absent(), Optional.absent(), () -> "true"));
            assertEquals(ImmutableList.of(wfDetails4), store.getLatestActiveWorkflowDefinitions(100, Optional.of(wfDetails3.getId()), Optional.absent(), () -> "true"));
            assertEmpty(anotherSite.getLatestActiveWorkflowDefinitions(100, Optional.absent(), Optional.absent(), () -> "true"));

            ////
            // public simple getters
            //
            assertEquals(proj1, store.getProjectById(proj1.getId()));
            assertEquals(proj2, store.getProjectById(proj2.getId()));
            assertNotFound(() -> store.getProjectById(proj1.getId() + 10));
            assertNotFound(() -> anotherSite.getProjectById(proj1.getId()));

            assertEquals(proj1, store.getProjectByName(proj1.getName()));
            assertEquals(proj2, store.getProjectByName(proj2.getName()));
            assertNotFound(() -> store.getProjectByName(proj1.getName() + " "));
            assertNotFound(() -> anotherSite.getProjectByName(proj1.getName()));

            assertEquals(rev1, store.getRevisionById(rev1.getId()));
            assertEquals(rev2, store.getRevisionById(rev2.getId()));
            assertNotFound(() -> store.getRevisionById(rev1.getId() + 10));
            assertNotFound(() -> anotherSite.getRevisionById(rev1.getId()));

            assertEquals(rev1, store.getRevisionByName(proj1.getId(), rev1.getName()));
            assertEquals(rev2, store.getRevisionByName(proj2.getId(), rev2.getName()));
            assertNotFound(() -> store.getRevisionByName(proj1.getId() + 10, rev1.getName()));
            assertNotFound(() -> store.getRevisionByName(proj1.getId(), rev2.getName()));
            assertNotFound(() -> anotherSite.getRevisionByName(proj1.getId(), rev1.getName()));

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
            assertEquals(rev1, store.getLatestRevision(proj1.getId()));
            assertEquals(rev3, store.getLatestRevision(proj2.getId()));
            assertNotFound(() -> anotherSite.getLatestRevision(proj1.getId()));

            assertEquals(wfDetails3, store.getLatestWorkflowDefinitionByName(proj2.getId(), wf3.getName()));
            assertNotFound(() -> store.getLatestWorkflowDefinitionByName(proj2.getId(), wf2.getName()));

            // getRevisionArchiveData returns NotFound if insertRevisionArchiveData is not called
            assertNotFound(() -> store.getRevisionArchiveData(rev1.getId()));

            ProjectMap projs = store.getProjectsByIdList(ImmutableList.of(proj1.getId(), proj2.getId()));
            assertEquals(proj1, projs.get(proj1.getId()));
            assertEquals(proj2, projs.get(proj2.getId()));
            assertNotFound(() -> projs.get(proj2.getId() + 10));

            TimeZoneMap defTimeZones = store.getWorkflowTimeZonesByIdList(ImmutableList.of(wf3.getId(), wf4.getId()));
            assertEquals(wf3.getTimeZone(), defTimeZones.get(wf3.getId()));
            assertEquals(wf4.getTimeZone(), defTimeZones.get(wf4.getId()));
            assertNotFound(() -> defTimeZones.get(wf2.getId()));
        });
    }

    @Test
    public void testGetActiveWorkflows()
            throws Exception
    {
        factory.begin(() -> {
            Project srcProj1 = Project.of("proj1");
            Revision srcRev1 = createRevision("rev1");
            WorkflowDefinition srcWf1 = createWorkflow("wf1");
            WorkflowDefinition srcWf2 = createWorkflow("test_wf2");
            WorkflowDefinition srcWf3 = createWorkflow("test_wf3");
            WorkflowDefinition srcWf4 = createWorkflow("wf%4");
            WorkflowDefinition srcWf5 = createWorkflow("wf_5");
            WorkflowDefinition srcWf6 = createWorkflow("wf%_6");
            final AtomicReference<StoredRevision> revRef = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRef1 = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRef2 = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRef3 = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRef4 = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRef5 = new AtomicReference<>();
            final AtomicReference<StoredWorkflowDefinition> wfRef6 = new AtomicReference<>();
            StoredProject proj1 = store.putAndLockProject(
                    srcProj1,
                    (store, stored) -> {
                        ProjectControl lock = new ProjectControl(store, stored);
                        assertNotConflict(() -> {
                            revRef.set(lock.insertRevision(srcRev1));
                            wfRef1.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(srcWf1), sm, Instant.now()).get(0));
                            wfRef2.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(srcWf2), sm, Instant.now()).get(0));
                            wfRef3.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(srcWf3), sm, Instant.now()).get(0));
                            wfRef4.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(srcWf4), sm, Instant.now()).get(0));
                            wfRef5.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(srcWf5), sm, Instant.now()).get(0));
                            wfRef6.set(lock.insertWorkflowDefinitions(revRef.get(), ImmutableList.of(srcWf6), sm, Instant.now()).get(0));
                        });
                        return lock.get();
                    });
            StoredWorkflowDefinition wf1 = wfRef1.get();
            StoredWorkflowDefinition wf2 = wfRef2.get();
            StoredWorkflowDefinition wf3 = wfRef3.get();
            StoredWorkflowDefinition wf4 = wfRef4.get();
            StoredWorkflowDefinition wf5 = wfRef5.get();
            StoredWorkflowDefinition wf6 = wfRef6.get();
            StoredWorkflowDefinitionWithProject wfDetails1 = StoredWorkflowDefinitionWithProject.of(wf1, proj1, srcRev1);
            StoredWorkflowDefinitionWithProject wfDetails2 = StoredWorkflowDefinitionWithProject.of(wf2, proj1, srcRev1);
            StoredWorkflowDefinitionWithProject wfDetails3 = StoredWorkflowDefinitionWithProject.of(wf3, proj1, srcRev1);
            StoredWorkflowDefinitionWithProject wfDetails4 = StoredWorkflowDefinitionWithProject.of(wf4, proj1, srcRev1);
            StoredWorkflowDefinitionWithProject wfDetails5 = StoredWorkflowDefinitionWithProject.of(wf5, proj1, srcRev1);
            StoredWorkflowDefinitionWithProject wfDetails6 = StoredWorkflowDefinitionWithProject.of(wf6, proj1, srcRev1);

            assertEquals(ImmutableList.of(wfDetails1, wfDetails2, wfDetails3, wfDetails4, wfDetails5, wfDetails6),
                    store.getLatestActiveWorkflowDefinitions(100, Optional.absent(), Optional.absent(), () -> "true"));
            assertEquals(ImmutableList.of(wfDetails1, wfDetails2, wfDetails3, wfDetails4, wfDetails5, wfDetails6),
                    store.getLatestActiveWorkflowDefinitions(100, Optional.absent(), Optional.fromNullable(""), () -> "true"));
            assertEquals(ImmutableList.of(wfDetails2, wfDetails3),
                    store.getLatestActiveWorkflowDefinitions(100, Optional.absent(), Optional.fromNullable("test"), () -> "true"));
            assertEquals(ImmutableList.of(wfDetails4, wfDetails6),
                    store.getLatestActiveWorkflowDefinitions(100, Optional.absent(), Optional.fromNullable("%"), () -> "true"));
            assertEquals(ImmutableList.of(wfDetails2, wfDetails3, wfDetails5, wfDetails6),
                    store.getLatestActiveWorkflowDefinitions(100, Optional.absent(), Optional.fromNullable("_"), () -> "true"));
            assertEquals(ImmutableList.of(wfDetails6),
                    store.getLatestActiveWorkflowDefinitions(100, Optional.absent(), Optional.fromNullable("%_"), () -> "true"));
            assertEquals(ImmutableList.of(),
                    store.getLatestActiveWorkflowDefinitions(100, Optional.absent(), Optional.fromNullable("*"), () -> "true"));
            // Check ascending parameter.
            assertEquals(ImmutableList.of(wfDetails6, wfDetails5, wfDetails4, wfDetails3, wfDetails2, wfDetails1),
                    store.getLatestActiveWorkflowDefinitions(100, Optional.absent(), false, Optional.absent(), false, () -> "true"));
            // Check searchProjectName parameter.
            assertEquals(ImmutableList.of(wfDetails6, wfDetails5, wfDetails4, wfDetails3, wfDetails2, wfDetails1),
                    store.getLatestActiveWorkflowDefinitions(100, Optional.absent(), false, Optional.fromNullable("proj1"), true, () -> "true"));
        });
    }

    @Test
    public void testRevisionArchiveData()
        throws Exception
    {
        factory.begin(() -> {
            byte[] data = "archive data".getBytes(UTF_8);

            StoredRevision rev = store.putAndLockProject(
                    Project.of("proj1"),
                    (store, stored) -> {
                        ProjectControl lock = new ProjectControl(store, stored);

                        StoredRevision storedRev = lock.insertRevision(createRevision("rev1"));
                        lock.insertRevisionArchiveData(storedRev.getId(), data);

                        return storedRev;
                    });

            assertArrayEquals(data, store.getRevisionArchiveData(rev.getId()));
            assertNotFound(() -> store.getRevisionArchiveData(rev.getId() + 10));
        });
    }

    @Test
    public void testDeleteProject()
        throws Exception
    {
        factory.begin(() -> {
            WorkflowDefinition srcWf1 = createWorkflow("wf1");
            AtomicReference<StoredWorkflowDefinition> wfRef = new AtomicReference<>();

            StoredRevision rev = store.putAndLockProject(
                    Project.of("proj1"),
                    (store, stored) -> {
                        ProjectControl lock = new ProjectControl(store, stored);

                        StoredRevision storedRev = lock.insertRevision(createRevision("rev1"));

                        wfRef.set(lock.insertWorkflowDefinitions(storedRev, ImmutableList.of(srcWf1), sm, Instant.now()).get(0));

                        return storedRev;
                    });

            StoredProject deletingProject = ProjectControl.deleteProject(store, rev.getProjectId(), (control, proj) -> {
                return proj;
            });

            assertEquals(deletingProject.getId(), rev.getProjectId());

            // lookup by name fails
            assertNotFound(() -> store.getProjectByName(deletingProject.getName()));

            // listing doesn't include deleted projects
            assertEquals(ImmutableList.of(), store.getProjects(100, Optional.absent(), Optional.absent(), () -> "true"));
            assertEquals(ImmutableList.of(), store.getProjectsWithLatestRevision(100, Optional.absent(), Optional.absent(), () -> "true"));
            assertEquals(ImmutableList.of(), store.getLatestActiveWorkflowDefinitions(100, Optional.absent(), Optional.absent(), () -> "true"));

            // lookup by project/revision id succeeds and deletedAt is set
            StoredProject deletedProj = store.getProjectById(deletingProject.getId());
            assertTrue(deletedProj.getDeletedAt().isPresent());

            List<StoredWorkflowDefinition> defs = store.getWorkflowDefinitions(rev.getId(), 100, Optional.absent(), () -> "true");
            assertEquals(1, defs.size());
            assertEquals(srcWf1, ImmutableWorkflowDefinition.builder().from(defs.get(0)).build());
            assertEquals(rev.getId(), defs.get(0).getRevisionId());

            // reusing same name is allowed
            StoredProject sameName = store.putAndLockProject(
                    Project.of("proj1"),
                    (store, stored) -> {
                        ProjectControl lock = new ProjectControl(store, stored);

                        StoredRevision storedRev = lock.insertRevision(createRevision("rev1"));

                        return stored;
                    });

            assertNotEquals(sameName.getId(), deletingProject.getId());
        });
    }

    @Test
    public void testGetProjects()
            throws Exception
    {
        factory.begin(() -> {
            StoredProject proj1 = createTestProject("proj1abc", "proj1_rev1", "proj1_wf1");
            StoredProject proj2 = createTestProject("proj2def", "proj2_rev1", "proj2_wf1");
            StoredProject proj3 = createTestProject("proj3ghi", "proj3_rev1", "proj3_wf1");
            StoredProject proj4 = createTestProject("other4jkl", "other4_rev1", "other4_wf1");
            List<StoredProject> projects;

            projects = store.getProjects(100, Optional.absent(), Optional.absent(), () -> "true");
            assertEquals(ImmutableList.of("proj1abc", "proj2def", "proj3ghi", "other4jkl"), projects.stream().map( (x) -> x.getName()).collect(Collectors.toList()));

            // Check pageSize
            projects = store.getProjects(2, Optional.absent(), Optional.absent(), () -> "true");
            assertEquals(ImmutableList.of("proj1abc", "proj2def"), projects.stream().map( (x) -> x.getName()).collect(Collectors.toList()));

            // Check lastId
            projects = store.getProjects(2, Optional.of(proj2.getId()), Optional.absent(), () -> "true");
            assertEquals(ImmutableList.of("proj3ghi", "other4jkl"), projects.stream().map( (x) -> x.getName()).collect(Collectors.toList()));

            // Check namePattern
            projects = store.getProjects(100, Optional.absent(), Optional.of("proj"), () -> "true");
            assertEquals(ImmutableList.of("proj1abc", "proj2def", "proj3ghi"), projects.stream().map( (x) -> x.getName()).collect(Collectors.toList()));

            // Check namePattern
            projects = store.getProjects(100, Optional.absent(), Optional.of("ghi"), () -> "true");
            assertEquals(ImmutableList.of("proj3ghi"), projects.stream().map( (x) -> x.getName()).collect(Collectors.toList()));

            // Check combination
            projects = store.getProjects(100, Optional.of(proj1.getId()), Optional.of("proj"), () -> "true");
            assertEquals(ImmutableList.of("proj2def", "proj3ghi"), projects.stream().map( (x) -> x.getName()).collect(Collectors.toList()));
        });
    }

    private StoredProject createTestProject(String projectName, String revision, String workflowName)
            throws ResourceConflictException
    {
        Project srcProj1 = Project.of(projectName);
        Revision srcRev1 = createRevision(revision);
        WorkflowDefinition srcWf1 = createWorkflow(workflowName);
        return store.putAndLockProject(
                srcProj1,
                (store, stored) -> {
                    ProjectControl lock = new ProjectControl(store, stored);
                    assertNotConflict(() -> {
                        StoredRevision rev1 = lock.insertRevision(srcRev1);
                        lock.insertWorkflowDefinitions(rev1, ImmutableList.of(srcWf1), sm, Instant.now());
                    });
                    return lock.get();
                });
    }
}
