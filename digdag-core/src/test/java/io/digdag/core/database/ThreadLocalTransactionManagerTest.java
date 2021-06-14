package io.digdag.core.database;

import io.digdag.core.repository.Project;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class ThreadLocalTransactionManagerTest
{
    private DatabaseFactory factory;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp()
            throws Exception
    {
        factory = DatabaseTestingUtils.setupDatabase();
    }

    @Test
    public void nestedTransactionIsNotAllowed()
            throws Exception
    {
        exception.expectMessage(containsString("Nested transaction is not allowed"));
        exception.expect(IllegalStateException.class);
        factory.get().begin(() -> {
            return factory.get().begin(() -> {
                fail();
                return null;
            });
        });
    }

    @Test
    public void reuseTransaction()
            throws Exception
    {
        Project proj1 = Project.of("proj1");

        factory.get().<Void, ResourceNotFoundException, ResourceConflictException>begin(() -> {
            factory.get().autoCommit(() -> {
                factory.getProjectStoreManager().getProjectStore(0)
                    .putAndLockProject(proj1, (store, stored) -> stored);
                return null;
            }, ResourceConflictException.class);

            // This transaction can read the stored project
            StoredProject proj2 = factory.getProjectStoreManager().getProjectStore(0)
                .getProjectByName("proj1");
            assertThat(proj2, is(notNullValue()));

            // Another transaction can't read it until committed
            try {
                StoredProject proj3 = CompletableFuture.supplyAsync(() -> {
                    return factory.get().autoCommit(() -> {
                        try {
                            return factory.getProjectStoreManager().getProjectStore(0)
                                .getProjectByName("proj1");
                        }
                        catch (ResourceNotFoundException ex) {
                            return null;
                        }
                    });
                }).get();
                assertThat(proj3, is(nullValue()));
            }
            catch (ExecutionException | InterruptedException ex) {
                assertThat(ex, is(nullValue()));
            }

            return null;
        }, ResourceNotFoundException.class, ResourceConflictException.class);

        // Another transaction can read after commit
        try {
            StoredProject proj4 = CompletableFuture.supplyAsync(() -> {
                return factory.get().autoCommit(() -> {
                    try {
                        return factory.getProjectStoreManager().getProjectStore(0)
                            .getProjectByName("proj1");
                    }
                    catch (ResourceNotFoundException ex) {
                        return null;
                    }
                });
            }).get();
            assertThat(proj4, is(notNullValue()));
        }
        catch (ExecutionException | InterruptedException ex) {
            assertThat(ex, is(nullValue()));
        }
    }
}
