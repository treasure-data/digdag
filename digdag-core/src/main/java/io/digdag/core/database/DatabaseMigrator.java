package io.digdag.core.database;

import java.util.List;
import java.util.ArrayList;
import com.google.inject.Inject;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.StatementException;

public class DatabaseMigrator
{
    private final DBI dbi;
    private final String databaseType;

    @Inject
    public DatabaseMigrator(DBI dbi, DatabaseConfig config)
    {
        this(dbi, config.getType());
    }

    public DatabaseMigrator(DBI dbi, String databaseType)
    {
        this.dbi = dbi;
        this.databaseType = databaseType;
    }

    public static String getDriverClassName(String type)
    {
        switch (type) {
        case "h2":
            return "org.h2.Driver";
        case "postgresql":
            return "org.postgresql.Driver";
        default:
            throw new RuntimeException("Unsupported database type: "+type);
        }
    }

    public String getSchemaVersion()
    {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("select name from schema_migrations order by name desc limit 1")
                .mapTo(String.class)
                .first();
        }
    }

    public void migrate()
    {
        String dbVersion = migrateSchemaVersions();
        for (Migration m : migrations) {
            if (dbVersion.compareTo(m.getVersion()) < 0) {
                try (Handle handle = dbi.open()) {
                    handle.inTransaction((h, session) -> {
                        m.migrate(h);
                        h.insert("insert into schema_migrations (name, created_at) values (?, now())", m.getVersion());
                        return true;
                    });
                }
            }
        }
    }

    private boolean isPostgres()
    {
        return databaseType.equals("postgresql");
    }

    private String migrateSchemaVersions()
    {
        String version;
        try {
            version = getSchemaVersion();
        }
        catch (StatementException ex) {
            // schema_migrations table not found
            try (Handle handle = dbi.open()) {
                handle.inTransaction((h, session) -> {
                    h.update(
                            new CreateTableBuilder("schema_migrations")
                            .addString("name", "not null")
                            .addTimestamp("created_at", "not null")
                            .build());
                    return true;
                });
            }
            version = getSchemaVersion();
        }
        if (version == null) {
            return "19700101000000";
        }
        else {
            return version;
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
            if (isPostgres()) {
                return add(column, "serial primary key");
            }
            else {
                return add(column, "int primary key AUTO_INCREMENT");
            }
        }

        public CreateTableBuilder addIntIdNoAutoIncrement(String column, String options)
        {
            if (isPostgres()) {
                return add(column, "serial primary key " + options);
            }
            else {
                return add(column, "int primary key AUTO_INCREMENT " + options);
            }
        }

        public CreateTableBuilder addLongId(String column)
        {
            if (isPostgres()) {
                return add(column, "bigserial primary key");
            }
            else {
                return add(column, "bigint primary key AUTO_INCREMENT");
            }
        }

        public CreateTableBuilder addLongIdNoAutoIncrement(String column, String options)
        {
            return add(column, "bigint primary key " + options);
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

        public CreateTableBuilder addUuid(String column, String options)
        {
            return add(column, "uuid " + options);
        }

        public CreateTableBuilder addString(String column, String options)
        {
            if (isPostgres()) {
                return add(column, "text " + options);
            }
            else {
                return add(column, "varchar(255) " + options);
            }
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
            if (isPostgres()) {
                return add(column, "bytea " + options);
            }
            else {
                return add(column, "varbinary(255) " + options);
            }
        }

        public CreateTableBuilder addLongBinary(String column, String options)
        {
            if (isPostgres()) {
                return add(column, "bytea " + options);
            }
            else {
                return add(column, "blob " + options);
            }
        }

        public CreateTableBuilder addTimestamp(String column, String options)
        {
            if (isPostgres()) {
                return add(column, "timestamp with time zone " + options);
            }
            else {
                return add(column, "timestamp " + options);
            }
        }

        public String build()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE " + name + " (\n");
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

    private final Migration MigrateCreateTables = new Migration() {
        @Override
        public String getVersion()
        {
            return "20151204221156";
        }

        @Override
        public void migrate(Handle handle)
        {
            if (isPostgres()) {
                // check existance of extension first because CREATE EXTENSION is allowed only for superuser
                String ver = handle.createQuery("select installed_version from pg_catalog.pg_available_extensions where name = 'uuid-ossp'")
                    .mapTo(String.class)
                    .first();
                if (ver == null) {
                    handle.update("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
                }
            }

            // repositories
            handle.update(
                    new CreateTableBuilder("repositories")
                    .addIntId("id")
                    .addInt("site_id", "not null")
                    .addString("name", "not null")
                    .addTimestamp("created_at", "not null")
                    //.addTimestamp("deleted_at", "not null")  // this points UNIXTIME 0 (1970-01-01 00:00:00 UTC) if this repository is not deleted
                    .build());
            handle.update("create unique index repositories_on_site_id_and_name on repositories (site_id, name)");
            //handle.update("create unique index repositories_on_site_id_and_name on repositories (site_id, name, deleted_at)");

            // revisions
            handle.update(
                    new CreateTableBuilder("revisions")
                    .addIntId("id")
                    .addInt("repository_id", "not null references repositories (id)")
                    .addString("name", "not null")
                    // TODO disabled flag
                    .addMediumText("default_params", "")  // TODO move this to revision_params as like workflow_configs
                    .addString("archive_type", "not null")
                    .addString("archive_path", "")
                    .addBinary("archive_md5", "")
                    .addTimestamp("created_at", "not null")
                    .build());
            handle.update("create unique index revisions_on_repository_id_and_name on revisions (repository_id, name)");
            handle.update("create index revisions_on_repository_id_and_id on revisions (repository_id, id desc)");

            // revision_archives
            handle.update(
                    new CreateTableBuilder("revision_archives")
                    .addIntIdNoAutoIncrement("id", "references revisions (id)")
                    .addLongBinary("archive_data", "not null")
                    .build());

            // workflow_configs
            handle.update(
                    new CreateTableBuilder("workflow_configs")
                    .addIntId("id")
                    .addInt("repository_id", "not null references repositories (id)")
                    .addMediumText("config", "not null")
                    .addLong("config_digest", "not null")
                    .build());
            handle.update("create index workflow_configs_on_repository_id_and_config_digest on workflow_configs (repository_id, config_digest)");

            // workflow_definitions
            handle.update(
                    new CreateTableBuilder("workflow_definitions")
                    .addLongId("id")
                    .addInt("config_id", "not null references workflow_configs (id)")
                    .addInt("revision_id", "not null references revisions (id)")
                    .addString("name", "not null")
                    .build());
            handle.update("create unique index workflow_definitions_on_revision_id_and_name on workflow_definitions (revision_id, name)");

            // schedules
            handle.update(
                    new CreateTableBuilder("schedules")
                    .addIntId("id")
                    .addInt("repository_id", "not null references repositories (id)")
                    .addLong("workflow_definition_id", "not null references workflow_definitions (id)")
                    .addLong("next_run_time", "not null")
                    .addLong("next_schedule_time", "not null")
                    .addLong("last_session_time", "")
                    .addString("timezone", "not null")
                    .addTimestamp("created_at", "not null")
                    .addTimestamp("updated_at", "not null")
                    .build());
            handle.update("create index schedules_on_repository_id on schedules (repository_id)");
            handle.update("create unique index schedules_on_workflow_definition_id on schedules (workflow_definition_id)");
            handle.update("create index schedules_on_next_run_time on schedules (next_run_time)");

            // sessions
            handle.update(
                    new CreateTableBuilder("sessions")
                    .addLongId("id")
                    .addInt("repository_id", "not null references repositories (id)")
                    .addString("workflow_name", "not null")
                    .addLong("session_time", "not null")
                    .addUuid("session_uuid", isPostgres() ? "not null default(uuid_generate_v4())" : "not null default(RANDOM_UUID())")
                    .addLong("last_attempt_id", "")
                    .build());
            handle.update("create unique index sessions_on_repository_id_and_workflow_name_and_session_time on sessions (repository_id, workflow_name, session_time)");
            handle.update("create index sessions_on_repository_id on sessions (repository_id, id)");

            // session_attempts
            handle.update(
                    new CreateTableBuilder("session_attempts")
                    .addLongId("id")
                    .addLong("session_id", "not null references sessions (id)")
                    .addInt("site_id", "not null")  // denormalized for performance
                    .addInt("repository_id", "not null references repositories (id)")  // denormalized for performance
                    .addString("attempt_name", "not null")
                    .addLong("workflow_definition_id", "references workflow_definitions (id)")
                    .addShort("state_flags", "not null")  // 0=running or blocked, 1=cancel_requested, 2=done, 4=success
                    .addString("timezone", "not null")
                    .addMediumText("params", "")
                    .addTimestamp("created_at", "not null")
                    .build());
            handle.update("create unique index session_attempts_on_session_id_and_attempt_name on session_attempts (session_id, attempt_name)");
            handle.update("create index session_attempts_on_site_id on session_attempts (site_id, id desc)");
            handle.update("create index session_attempts_on_workflow_definition_id on session_attempts (workflow_definition_id, id desc)");
            handle.update("create index session_attempts_on_repository_id on session_attempts (repository_id, id desc)");

            // task_archives
            handle.update(
                    new CreateTableBuilder("task_archives")
                    .addLongIdNoAutoIncrement("id", "references session_attempts (id)")
                    .addLongText("tasks", "not null")  // collection of tasks, delete tasks transactionally when archived
                    .addTimestamp("created_at", "not null")
                    .build());

            // session_monitors
            handle.update(
                    new CreateTableBuilder("session_monitors")
                    .addLongId("id")
                    .addLong("attempt_id", "not null")
                    .addLong("next_run_time", "not null")
                    .addString("type", "not null")
                    .addMediumText("config", "")
                    .addTimestamp("created_at", "not null")
                    .addTimestamp("updated_at", "not null")
                    .build());
            handle.update("create index session_monitors_on_attempt_id on session_monitors (attempt_id)");
            handle.update("create index session_monitors_on_next_run_time on session_monitors (next_run_time)");

            // tasks
            handle.update(
                    new CreateTableBuilder("tasks")
                    .addLongId("id")
                    .addLong("attempt_id", "not null references session_attempts (id)")
                    .addLong("parent_id", "references tasks (id)")
                    .addShort("task_type", "not null")   // 0=action, 1=grouping
                    //.addShort("error_mode", "not null")  // 1=ignore_parent_flags
                    .addShort("state", "not null")
                    .addShort("state_flags", "not null")
                    .addTimestamp("updated_at", "not null")  // last state update is done at this time
                    .addTimestamp("retry_at", "")
                    .addMediumText("state_params", "")
                    .build());
            handle.update("create index tasks_on_attempt_id on tasks (attempt_id, id)");
            handle.update("create index tasks_on_parent_id_and_state on tasks (parent_id, state)");
            if (isPostgres()) {
                // for findTasksByState(BLOCKED) at propagateAllBlockedToReady
                // for findTasksByState(PLANNED) at propagateAllPlannedToDone
                // for findTasksByState(READY) through findAllReadyTaskIds() at enqueueReadyTasks
                handle.update("create index tasks_on_state_and_id on tasks (state, id) where state = 0 or state = 1 or state = 5");
            }
            else {
                // for findTasksByState
                handle.update("create index tasks_on_state_and_id on tasks (state, id)");
            }

            handle.update(
                    new CreateTableBuilder("task_details")
                    .addLongIdNoAutoIncrement("id", "references tasks (id)")
                    .addMediumText("full_name", "not null")
                    .addMediumText("local_config", "")
                    .addMediumText("export_config", "")
                    .build());

            handle.update(
                    new CreateTableBuilder("task_state_details")
                    .addLongIdNoAutoIncrement("id", "references tasks (id)")
                    .addMediumText("subtask_config", "")
                    .addMediumText("export_params", "")
                    .addMediumText("store_params", "")
                    .addMediumText("report", "")
                    .addMediumText("error", "")
                    .build());

            // task_dependencies
            handle.update(
                    new CreateTableBuilder("task_dependencies")
                    .addLongId("id")
                    .addLong("upstream_id", "not null")
                    .addLong("downstream_id", "not null")
                    .build());
            handle.update("create index task_dependencies_on_downstream_id on task_dependencies (downstream_id)");

            // queue_settings
            handle.update(
                    new CreateTableBuilder("queue_settings")
                    .addIntId("id")
                    .addInt("site_id", "not null")
                    .addString("name", "not null")
                    .addMediumText("config", "")
                    .addTimestamp("created_at", "not null")
                    .addTimestamp("updated_at", "not null")
                    .build());
            handle.update("create unique index queue_settings_on_site_id_and_name on queue_settings (site_id, name)");
            handle.update("create index queue_settings_on_site_id on queue_settings (site_id, id)");

            // queues
            handle.update(
                    new CreateTableBuilder("queues")
                    .addIntIdNoAutoIncrement("id", "references queue_settings (id)")
                    .addInt("max_concurrency", "not null")
                    .build());

            // resource_types
            handle.update(
                    new CreateTableBuilder("resource_types")
                    .addIntId("id")
                    .addInt("queue_id", "not null references queues (id)")
                    .addInt("max_concurrency", "not null")
                    .addString("name", "not null")
                    .build());
            handle.update("create unique index resource_types_on_queue_id_and_name on resource_types (queue_id, name)");

            // queued_tasks
            handle.update(
                    new CreateTableBuilder("queued_tasks")
                    .addLongId("id")
                    .addInt("site_id", "not null")  // denormalized for performance
                    .addInt("queue_id", "not null")
                    .addInt("priority", "not null")
                    .addInt("resource_type_id", "")
                    .addLong("task_id", "not null")
                    .addTimestamp("created_at", "not null")
                    .addLongBinary("data", "not null")
                    .build());
            handle.update("create unique index queued_tasks_on_queue_id_task_id on queued_tasks (queue_id, task_id)");

            // queued_shared_task_locks
            handle.update(
                    new CreateTableBuilder("queued_shared_task_locks")
                    .addLongId("id")  // references queued_tasks.id
                    .addInt("queue_id", "not null")
                    .addInt("priority", "not null")
                    .addInt("resource_type_id", "")
                    .addInt("retry_count", "not null")
                    .addLong("hold_expire_time", "")
                    .addString("hold_agent_id", "")
                    .build());

            // queued_task_locks
            handle.update(
                    new CreateTableBuilder("queued_task_locks")
                    .addLongId("id")  // references queued_tasks.id
                    .addInt("queue_id", "not null")
                    .addInt("priority", "not null")
                    .addInt("resource_type_id", "")
                    .addInt("retry_count", "not null")
                    .addLong("hold_expire_time", "")
                    .addString("hold_agent_id", "")
                    .build());

            if (isPostgres()) {
                handle.update("create index queued_shared_task_locks_grouping on queued_shared_task_locks (queue_id, resource_type_id) where hold_expire_time is not null");
                handle.update("create index queued_shared_task_locks_ordering on queued_shared_task_locks (queue_id, priority desc, id) where hold_expire_time is null");
                handle.update("create index queued_shared_task_locks_expiration on queued_shared_task_locks (hold_expire_time) where hold_expire_time is not null");
                handle.update("create index queued_task_locks_grouping on queued_task_locks (queue_id, resource_type_id) where hold_expire_time is not null");
                handle.update("create index queued_task_locks_ordering on queued_task_locks (queue_id, priority desc, id) where hold_expire_time is null");
                handle.update("create index queued_task_locks_expiration on queued_task_locks (hold_expire_time) where hold_expire_time is not null");
            }
            else {
                handle.update("create index queued_shared_task_locks_grouping on queued_shared_task_locks (hold_expire_time, queue_id, resource_type_id)");
                handle.update("create index queued_shared_task_locks_ordering on queued_shared_task_locks (queue_id, hold_expire_time, priority desc, id)");
                handle.update("create index queued_task_locks_grouping on queued_task_locks (hold_expire_time, queue_id, resource_type_id)");
                handle.update("create index queued_task_locks_ordering on queued_task_locks (queue_id, hold_expire_time, priority desc, id)");
            }
        }
    };

    private final Migration[] migrations = {
        MigrateCreateTables,
    };
}
