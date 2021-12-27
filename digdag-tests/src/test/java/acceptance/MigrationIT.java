package acceptance;

import io.digdag.core.database.DatabaseMigrator;
import io.digdag.core.database.migrate.Migration;
import io.digdag.core.database.migrate.MigrationContext;
import io.digdag.core.database.migrate.Migration_20151204221156_CreateTables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.Handle;
import utils.TemporaryDigdagServer;

import javax.sql.DataSource;
import java.sql.Connection;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;


public class MigrationIT
{
    public TemporaryDigdagServer server = null;

    @Before
    public void setUp()
    {
        server = TemporaryDigdagServer.of();
    }

    @After
    public void tearDown()
    {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    /**
     * Check the behavior for upgrading existing database
     * @throws Exception
     */
    @Test
    public void checkDatabaseUpgrade()
    {
        assumeTrue(server.isRemoteDatabase());

        try {
            server.setupDatabase();
            DataSource ds = server.getTestDBDataSource();
            Jdbi dbi = Jdbi.create(ds);
            DatabaseMigrator migrator = new DatabaseMigrator(dbi, server.getRemoteTestDatabaseConfig());
            MigrationContext context = new MigrationContext(migrator.getDatabaseType());

            //Prepare for tables
            try (Handle handle = dbi.open()) {
                migrator.createSchemaMigrationsTable(handle, context);
                Migration m = new Migration_20151204221156_CreateTables();
                migrator.applyMigration(m, handle, context);
            }

            //Apply rest migrations when digdag server start
            server.start(true);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

    /**
     * Check session_attempts_on_site_id_and_state_flags_partial_2 index exists
     * @throws Exception
     */
    @Test
    public void checkMigration_20190318175338_AddIndexToSessionAttempts()
    {
        assumeTrue(server.isRemoteDatabase());

        try {
            server.start();
            DataSource ds = server.getTestDBDataSource();
            Connection con = ds.getConnection();
            con.createStatement().execute("drop index session_attempts_on_site_id_and_state_flags_partial_2");
        }
        catch (Exception e) {
            fail(e.toString());
        }
    }
}
