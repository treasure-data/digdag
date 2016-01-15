package io.digdag.core.database;

import java.util.List;
import java.util.ArrayList;
import com.google.inject.Inject;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Handle;

public class DatabaseMigrator
{
    private final IDBI dbi;
    private final String databaseType;

    @Inject
    public DatabaseMigrator(IDBI dbi, DatabaseStoreConfig config)
    {
        this(dbi, config.getType());
    }

    public DatabaseMigrator(IDBI dbi, String databaseType)
    {
        this.dbi = dbi;
        this.databaseType = databaseType;
    }

    public static String getDriverClassName(String type)
    {
        switch (type) {
        case "h2":
            return "org.h2.Driver";
        default:
            throw new RuntimeException("Unsupported database type: "+type);
        }
    }

    public String getDatabaseVersion()
    {
        try (Handle handle = dbi.open()) {
            if (checkTableExists(handle, "schema_migrations")) {
                String version = handle.createQuery("select version from schema_migrations order by name desc limit 1")
                    .mapTo(String.class)
                    .first();
                if (version != null) {
                    return version;
                }
            }
            return "19700101000000";
        }
    }

    private static boolean checkTableExists(Handle handle, String name)
    {
        try {
            Connection con = handle.getConnection();
            DatabaseMetaData meta = con.getMetaData();
            ResultSet res = meta.getTables(null, null, "schema_migrations", new String[] {"TABLE"});
            return res.next();
        }
        catch (SQLException ex) {
            throw new RuntimeException("Unable to get table list", ex);
        }
    }

    private class CreateTableBuilder
    {
        private final String name;
        private final List<String> columns = new ArrayList<>();

        public CreateTableBuilder(String name)
        {
            this.name = name;
        }

        public CreateTableBuilder add(String column, String typeAndOptions)
        {
            columns.add(column + " " + typeAndOptions);
            return this;
        }

        public CreateTableBuilder addIntId(String column)
        {
            return add(column, "int primary key AUTO_INCREMENT");
        }

        public CreateTableBuilder addLongId(String column)
        {
            return add(column, "bigint primary key AUTO_INCREMENT");
        }

        public CreateTableBuilder addShort(String column, String options)
        {
            return add(column, "smallint " + options);
        }

        public CreateTableBuilder addInt(String column, String options)
        {
            return add(column, "int " + options);
        }

        public CreateTableBuilder addLong(String column, String options)
        {
            return add(column, "bigint " + options);
        }

        public CreateTableBuilder addString(String column, String options)
        {
            return add(column, "varchar(255) " + options);
        }

        public CreateTableBuilder addMediumText(String column, String options)
        {
            return add(column, "text " + options);
        }

        public CreateTableBuilder addLongText(String column, String options)
        {
            return add(column, "text " + options);
        }

        public CreateTableBuilder addBinary(String column, String options)
        {
            return add(column, "varbinary(255) " + options);
        }

        public CreateTableBuilder addLongBinary(String column, String options)
        {
            return add(column, "blob " + options);
        }

        public CreateTableBuilder addTimestamp(String column, String options)
        {
            return add(column, "timestamp " + options);
        }

        public String build()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE IF NOT EXISTS " + name + " (\n");
            for (int i=0; i < columns.size(); i++) {
                sb.append("  ");
                sb.append(columns.get(i));
                if (i + 1 < columns.size()) {
                    sb.append(",\n");
                } else {
                    sb.append("\n");
                }
            }
            sb.append(")");
            return sb.toString();
        }
    }

    private interface Migration
    {
        public String getVersion();

        public void migrate(Handle handle);
    }

    private final Migration MigrateCreateSchemaVersions = new Migration() {
        @Override
        public String getVersion()
        {
            return "20151204215653";
        }

        @Override
        public void migrate(Handle handle)
        {
            handle.update(
                    new CreateTableBuilder("schema_migrations")
                    .addString("name", "not null")
                    .addTimestamp("created_at", "not null")
                    .build());
        }
    };

    private final Migration MigrateCreateTables = new Migration() {
        @Override
        public String getVersion()
        {
            return "20151204221156";
        }

        @Override
        public void migrate(Handle handle)
        {
            // TODO references

            // repositories
            handle.update(
                    new CreateTableBuilder("repositories")
                    .addIntId("id")
                    .addInt("site_id", "not null")
                    .addString("name", "not null")
                    //.addMediumText("config", "")
                    //.addBoolean("disabled", "not null")
                    //.addInt("latest_revision_id", "")
                    .addTimestamp("created_at", "not null")
                    .addTimestamp("updated_at", "not null")
                    .build());
            handle.update("create unique index if not exists repositories_on_site_id_and_name on repositories (site_id, name)");
            handle.update("create index if not exists repositories_on_site_id_and_id on repositories (site_id, id)");

            // revisions
            handle.update(
                    new CreateTableBuilder("revisions")
                    .addIntId("id")
                    // TODO disabled flag
                    .addInt("repository_id", "not null")
                    .addString("name", "not null")
                    .addMediumText("global_params", "")
                    .addString("archive_type", "not null")
                    .addBinary("archive_md5", "")
                    .addString("archive_path", "")
                    .addLongBinary("archive_data", "")  // TODO move to revision_archives
                    .addTimestamp("created_at", "not null")
                    .build());
            handle.update("create unique index if not exists revisions_on_repository_id_and_name on revisions (repository_id, name)");
            handle.update("create index if not exists revisions_on_repository_id_and_id on revisions (repository_id, id)");

            //handle.update(
            //        new CreateTableBuilder("revision_archives")
            //        .addIntId("id")
            //        .addLongBinary("archive_data", "")
            //        .build());

            // workflow_sources
            handle.update(
                    new CreateTableBuilder("workflow_sources")
                    .addIntId("id")
                    .addInt("revision_id", "not null")
                    .addString("name", "not null")
                    .addMediumText("config", "")
                    .build());
            handle.update("create unique index if not exists workflows_on_revision_id_and_name on workflow_sources (revision_id, name)");
            handle.update("create index if not exists workflows_on_revision_id_and_id on workflow_sources (revision_id, id)");

            // schedule_sources
            handle.update(
                    new CreateTableBuilder("schedule_sources")
                    .addIntId("id")
                    .addInt("revision_id", "not null")
                    .addString("name", "not null")
                    .addMediumText("config", "")
                    .build());
            handle.update("create unique index if not exists workflows_on_revision_id_and_name on schedule_sources (revision_id, name)");
            handle.update("create index if not exists workflows_on_revision_id_and_id on schedule_sources (revision_id, id)");

            // schedules
            handle.update(
                    new CreateTableBuilder("schedules")
                    .addLongId("id")
                    .addInt("workflow_id", "not null")
                    .addMediumText("config", "")
                    .addLong("next_run_time", "not null")
                    .addLong("next_schedule_time", "not null")
                    .addTimestamp("created_at", "not null")
                    .addTimestamp("updated_at", "not null")
                    .build());
            handle.update("create unique index if not exists schedules_on_workflow_id on schedules (workflow_id)");
            handle.update("create index if not exists schedules_on_next_run_time on schedules (next_run_time)");

            // queues
            handle.update(
                    new CreateTableBuilder("queues")
                    .addIntId("id")
                    .addInt("site_id", "not null")
                    .addString("name", "not null")
                    .addMediumText("config", "")
                    .addTimestamp("created_at", "not null")
                    .addTimestamp("updated_at", "not null")
                    .build());
            handle.update("create unique index if not exists queues_on_site_id_and_name on queues (site_id, name)");
            handle.update("create index if not exists queues_on_site_id_and_id on queues (site_id, id)");

            // sessions
            handle.update(
                    new CreateTableBuilder("sessions")
                    .addLongId("id")
                    .addInt("site_id", "not null")
                    .addShort("namespace_type", "not null")  // 0=site_id, 1=repository_id, 2=revision_id, 3=workflow_id
                    .addInt("namespace_id", "not null")      // site_id or repository_id if one-time workflow, otherwise workflow_id
                    // TODO task_index
                    .addString("name", "not null")
                    .addMediumText("params", "")
                    .addMediumText("options", "")    // TODO set in params? or rename to config?
                    .addTimestamp("created_at", "not null")
                    .build());
            handle.update("create unique index if not exists sessions_on_namespace_and_name on sessions (namespace_id, name, namespace_type)");
            handle.update("create index if not exists sessions_on_site_id_and_id on sessions (site_id, id)");

            // session_monitors
            handle.update(
                    new CreateTableBuilder("session_monitors")
                    .addLongId("id")
                    .addInt("session_id", "not null")
                    .addMediumText("config", "not null")
                    .addLong("next_run_time", "not null")
                    .addTimestamp("created_at", "not null")
                    .addTimestamp("updated_at", "not null")
                    .build());
            handle.update("create index if not exists session_monitors_on_workflow_id on session_monitors (session_id)");
            handle.update("create index if not exists session_monitors_on_next_run_time on session_monitors (next_run_time)");

            // session_archives
            handle.update(
                    new CreateTableBuilder("session_archives")
                    .addLongId("id")
                    // TODO save state
                    .addLongText("tasks", "")  // collection of tasks, delete tasks transactionally when archived
                    .addTimestamp("updated_at", "not null")
                    .build());

            // session_relations
            handle.update(
                    new CreateTableBuilder("session_relations")
                    .addLongId("id")  // references sessions.id
                    .addInt("repository_id", "not null")
                    .addInt("revision_id", "not null")
                    .addInt("workflow_id", "")       // null if one-time associated-to-revision workflow
                    .build());
            handle.update("create index if not exists session_relations_on_repository_id_and_id on session_relations (repository_id, id)");
            handle.update("create index if not exists session_relations_on_revision_id_and_id on session_relations (revision_id, id)");
            handle.update("create index if not exists session_relations_on_workflow_id_and_id on session_relations (workflow_id, id)");

            // tasks
            handle.update(
                    new CreateTableBuilder("tasks")
                    .addLongId("id")
                    .addLong("session_id", "not null")
                    .addLong("parent_id", "")
                    .addShort("task_type", "")   // 0=action, 1=grouping  NOT NULL
                    //.addShort("error_mode", "")  // 1=ignore_parent_flags  NOT NULL
                    .addShort("state", "not null")
                    .addShort("state_flags", "not null")
                    .addTimestamp("updated_at", "not null")  // last state update is done at this time
                    .addTimestamp("retry_at", "")
                    .build());

            handle.update(
                    new CreateTableBuilder("task_details")
                    .addLongId("id")
                    .addMediumText("full_name", "not null")
                    .addMediumText("local_config", "")
                    .addMediumText("export_config", "")
                    .build());

            handle.update(
                    new CreateTableBuilder("task_state_details")
                    .addLongId("id")
                    .addMediumText("state_params", "")
                    .addMediumText("carry_params", "")
                    .addMediumText("error", "")
                    .addMediumText("report", "")
                    .build());

            // task_dependencies
            handle.update(
                    new CreateTableBuilder("task_dependencies")
                    .addLongId("id")
                    .addLong("upstream_id", "")
                    .addLong("downstream_id", "")
                    .build());
            handle.update("create index if not exists task_dependencies_on_downstream_id on task_dependencies (downstream_id)");
        }
    };

    private final Migration[] migrations = {
        MigrateCreateSchemaVersions,
        MigrateCreateTables,
    };

    public void migrate()
    {
        String dbVersion = getDatabaseVersion();
        for (Migration m : migrations) {
            if (dbVersion.compareTo(m.getVersion()) < 0) {
                try (Handle handle = dbi.open()) {
                    m.migrate(handle);
                    handle.insert("insert into schema_migrations (name, created_at) values (?, now())", m.getVersion());
                }
            }
        }
    }
}
