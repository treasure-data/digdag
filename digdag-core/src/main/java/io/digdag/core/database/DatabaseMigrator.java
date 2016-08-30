package io.digdag.core.database;

import com.google.inject.Inject;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.StatementException;

import java.util.ArrayList;
import java.util.List;

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

            // projects
            handle.update(
                    new CreateTableBuilder("projects")
                    .addIntId("id")
                    .addInt("site_id", "not null")
                    .addString("name", "not null")
                    .addTimestamp("created_at", "not null")
                    //.addTimestamp("deleted_at", "not null")  // this points UNIXTIME 0 (1970-01-01 00:00:00 UTC) if this project is not deleted
                    .build());
            handle.update("create unique index projects_on_site_id_and_name on projects (site_id, name)");
            //handle.update("create unique index projects_on_site_id_and_name on projects (site_id, name, deleted_at)");

            // revisions
            handle.update(
                    new CreateTableBuilder("revisions")
                    .addIntId("id")
                    .addInt("project_id", "not null references projects (id)")
                    .addString("name", "not null")
                    // TODO disabled flag
                    .addMediumText("default_params", "")  // TODO move this to revision_params as like workflow_configs
                    .addString("archive_type", "not null")
                    .addString("archive_path", "")
                    .addBinary("archive_md5", "")
                    .addTimestamp("created_at", "not null")
                    .build());
            handle.update("create unique index revisions_on_project_id_and_name on revisions (project_id, name)");
            handle.update("create index revisions_on_project_id_and_id on revisions (project_id, id desc)");

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
                    .addInt("project_id", "not null references projects (id)")
                    .addLong("config_digest", "not null")
                    .addString("timezone", "not null")
                    .addMediumText("config", "not null")
                    .build());
            handle.update("create index workflow_configs_on_project_id_and_config_digest on workflow_configs (project_id, config_digest)");

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
                    .addInt("project_id", "not null references projects (id)")
                    .addLong("workflow_definition_id", "not null references workflow_definitions (id)")
                    .addLong("next_run_time", "not null")
                    .addLong("next_schedule_time", "not null")
                    .addLong("last_session_time", "")
                    .addTimestamp("created_at", "not null")
                    .addTimestamp("updated_at", "not null")
                    .build());
            handle.update("create index schedules_on_project_id on schedules (project_id)");
            handle.update("create unique index schedules_on_workflow_definition_id on schedules (workflow_definition_id)");
            handle.update("create index schedules_on_next_run_time on schedules (next_run_time)");

            // sessions
            handle.update(
                    new CreateTableBuilder("sessions")
                    .addLongId("id")
                    .addInt("project_id", "not null references projects (id)")
                    .addString("workflow_name", "not null")
                    .addLong("session_time", "not null")
                    .addUuid("session_uuid", isPostgres() ? "not null default(uuid_generate_v4())" : "not null default(RANDOM_UUID())")
                    .addLong("last_attempt_id", "")
                    .build());
            handle.update("create unique index sessions_on_project_id_and_workflow_name_and_session_time on sessions (project_id, workflow_name, session_time)");
            handle.update("create index sessions_on_project_id on sessions (project_id, id)");

            // session_attempts
            handle.update(
                    new CreateTableBuilder("session_attempts")
                    .addLongId("id")
                    .addLong("session_id", "not null references sessions (id)")
                    .addInt("site_id", "not null")  // denormalized for performance
                    .addInt("project_id", "not null references projects (id)")  // denormalized for performance
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
            handle.update("create index session_attempts_on_project_id on session_attempts (project_id, id desc)");

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

    private final Migration MigrateSessionsOnProjectIdIndexToDesc = new Migration() {
        @Override
        public String getVersion()
        {
            return "20160602123456";
        }

        @Override
        public void migrate(Handle handle)
        {
            handle.update("create index sessions_on_project_id_desc on sessions (project_id, id desc)");
            handle.update("drop index sessions_on_project_id");
        }
    };

    private final Migration MigrateCreateResumingTasks = new Migration() {
        @Override
        public String getVersion()
        {
            return "20160602184025";
        }

        @Override
        public void migrate(Handle handle)
        {
            // resuming_tasks
            handle.update(
                    new CreateTableBuilder("resuming_tasks")
                    .addLongId("id")
                    .addLong("attempt_id", "not null references session_attempts (id)")
                    .addLong("source_task_id", "not null")
                    .addMediumText("full_name", "not null")
                    .addTimestamp("updated_at", "not null")
                    .addMediumText("local_config", "")
                    .addMediumText("export_config", "")
                    .addMediumText("subtask_config", "")
                    .addMediumText("export_params", "")
                    .addMediumText("store_params", "")
                    .addMediumText("report", "")
                    .addMediumText("error", "")
                    .build());
            if (isPostgres()) {
                handle.update("create index resuming_tasks_on_attempt_id_and_full_name on resuming_tasks (attempt_id, full_name)");
            }
            else {
                // h2 doesn't support index on text
                handle.update("create index resuming_tasks_on_attempt_id on resuming_tasks (attempt_id)");
            }

            // task_details.resuming_task_id
            handle.update("alter table task_details" +
                    " add column resuming_task_id bigint");
        }
    };

    private final Migration MigrateMakeProjectsDeletable = new Migration() {
        @Override
        public String getVersion()
        {
            return "20160610154832";
        }

        @Override
        public void migrate(Handle handle)
        {
            if (isPostgres()) {
                handle.update("alter table projects" +
                        " add column deleted_at timestamp with time zone");
                handle.update("alter table projects" +
                        " add column deleted_name text");
                handle.update("alter table projects" +
                        " alter column name drop not null");
            }
            else {
                handle.update("alter table projects" +
                        " add column deleted_at timestamp");
                handle.update("alter table projects" +
                        " add column deleted_name varchar(255)");
                handle.update("alter table projects" +
                        " alter column name drop not null");
            }
        }
    };

    private final Migration MigrateAddUserInfoColumnToRevisions = new Migration()
    {
        @Override
        public String getVersion()
        {
            return "20160623123456";
        }

        @Override
        public void migrate(Handle handle)
        {
            handle.update("alter table revisions" +
                    " add column user_info text");
        }
    };

    private final Migration MigrateQueueRearchitecture = new Migration() {
        @Override
        public String getVersion()
        {
            return "20160719172538";
        }

        @Override
        public void migrate(Handle handle)
        {
            // queued_task_locks
            handle.update("drop table queued_task_locks");
            handle.update("drop table queued_shared_task_locks");

            handle.update(
                    new CreateTableBuilder("queued_task_locks")
                    .addLongId("id")  // references queued_tasks.id
                    .addInt("site_id", "")
                    .addInt("queue_id", "")
                    .addInt("priority", "not null")
                    .addInt("retry_count", "not null default 0")
                    .addLong("lock_expire_time", "")
                    .addString("lock_agent_id", "")
                    .build());

            handle.update("insert into queued_task_locks" +
                    " (id, site_id, queue_id, priority)" +
                    " select id, site_id, NULL, priority" +
                    " from queued_tasks");

            // queues
            handle.update("alter table queues" +
                    " add column shared_site_id int");

            // resource_types
            handle.update("drop table resource_types");

            // queued_tasks
            handle.update("alter table queued_tasks" +
                    " alter column task_id drop not null");
            handle.update("alter table queued_tasks" +
                    " alter column queue_id drop not null");
            handle.update("alter table queued_tasks" +
                    " alter column data drop not null");
            handle.update("alter table queued_tasks" +
                    " drop column priority");
            handle.update("alter table queued_tasks" +
                    " drop column resource_type_id");
            handle.update("create unique index queued_tasks_on_site_id_task_id on queued_tasks (site_id, task_id)");

            if (isPostgres()) {
                handle.update(
                    "CREATE FUNCTION lock_shared_tasks(target_site_id int, target_site_max_concurrency bigint, limit_count int, lock_expire_seconds int, agent_id text) returns setof bigint as $$\n" +
                    "BEGIN\n" +
                    "  IF pg_try_advisory_xact_lock(23300, target_site_id) THEN\n" +
                    "    RETURN QUERY\n" +
                    "      with updated as (\n" +
                    "        update queued_task_locks\n" +
                    "        set lock_expire_time = cast(extract(epoch from statement_timestamp()) as bigint) + lock_expire_seconds,\n" +
                    "            lock_agent_id = agent_id\n" +
                    "        where id = any(\n" +
                    "          select queued_task_locks.id\n" +
                    "          from queued_task_locks\n" +
                    "          where lock_expire_time is null\n" +
                    "          and site_id = target_site_id\n" +
                    "          and not exists (\n" +
                    "            select * from (\n" +
                    "              select queue_id, count(*) as count\n" +
                    "              from queued_task_locks\n" +
                    "              where lock_expire_time is not null\n" +
                    "                and site_id = target_site_id\n" +
                    "              group by queue_id\n" +
                    "            ) runnings\n" +
                    "            join queues on queues.id = runnings.queue_id\n" +
                    "            where runnings.count >= queues.max_concurrency\n" +
                    "              and runnings.queue_id = queued_task_locks.queue_id\n" +
                    "          )\n" +
                    "          and not exists (\n" +
                    "            select count(*)\n" +
                    "            from queued_task_locks\n" +
                    "            where lock_expire_time is not null\n" +
                    "              and site_id = target_site_id\n" +
                    "            having count(*) >= target_site_max_concurrency\n" +
                    "          )\n" +
                    "          order by queue_id, priority desc, id\n" +
                    "          limit limit_count\n" +
                    "        )\n" +
                    "        returning queue_id, priority, id\n" +
                    "      )\n" +
                    "      select id from updated\n" +
                    "      order by queue_id, priority desc, id;\n" +
                    "  END IF;\n" +
                    "END;\n" +
                    "$$ LANGUAGE plpgsql VOLATILE\n" +
                "");

                handle.update("create index queued_tasks_shared_grouping on queued_task_locks (site_id, queue_id) where site_id is not null and lock_expire_time is not null");
                handle.update("create index queued_tasks_ordering on queued_task_locks (site_id, queue_id, priority desc, id) where lock_expire_time is null");
                handle.update("create index queued_tasks_expiration on queued_task_locks (lock_expire_time) where lock_expire_time is not null");
            }
            else {
                handle.update("create index queued_tasks_shared_grouping on queued_task_locks (lock_expire_time, site_id, queue_id)");
                handle.update("create index queued_tasks_ordering on queued_task_locks (site_id, queue_id, lock_expire_time, priority desc, id)");
            }
        }
    };

    private final Migration MigrateAddSecretsTable = new Migration()
    {
        @Override
        public String getVersion()
        {
            return "20160817123456";
        }

        @Override
        public void migrate(Handle handle)
        {
            handle.update(
                    new CreateTableBuilder("secrets")
                            .addLongId("id")
                            .addLong("site_id", "not null")
                            .addLong("project_id", "not null references projects (id)")
                            .addString("scope", "not null")
                            .addString("engine", "not null")
                            .addString("key", "not null")
                            .addLongText("value", "not null")
                            .addTimestamp("updated_at", "not null")
                            .build());

        }
    };

    private final Migration MigrateAddFinishedAtToSessionAttempts = new Migration() {
        @Override
        public String getVersion()
        {
            return "20160818043815";
        }

        @Override
        public void migrate(Handle handle)
        {
            if (isPostgres()) {
                handle.update("alter table session_attempts" +
                        " add column finished_at timestamp with time zone");
            }
            else {
                handle.update("alter table session_attempts" +
                        " add column finished_at timestamp");
            }
        }
    };

    private final Migration MigrateQueueUniqueName = new Migration() {
        @Override
        public String getVersion()
        {
            return "20160818220026";
        }

        @Override
        public void migrate(Handle handle)
        {
            handle.update("drop index queued_tasks_on_queue_id_task_id");
            handle.update("drop index queued_tasks_on_site_id_task_id");

            // alter "task_id bigint" to "unique_name text"
            if (isPostgres()) {
                handle.update("alter table queued_tasks" +
                        " alter column task_id type text");
                handle.update("alter table queued_tasks" +
                            " rename column task_id to unique_name");
            }
            else {
                handle.update("alter table queued_tasks" +
                        " alter column task_id varchar(255)");
                handle.update("alter table queued_tasks" +
                            " alter column task_id rename to unique_name");
            }

            handle.update("alter table queued_tasks" +
                        " alter unique_name set not null");

            handle.update("create unique index queued_tasks_on_site_id_unique_name on queued_tasks (site_id, unique_name)");
            if (isPostgres()) {
                handle.update("create unique index queued_tasks_on_queue_id_unique_name on queued_tasks (queue_id, unique_name) where queue_id is not null");
            }
            else {
                handle.update("create unique index queued_tasks_on_queue_id_unique_name on queued_tasks (queue_id, unique_name)");
            }

            handle.update("alter table tasks" +
                    " add column retry_count int default 0");
        }
    };

    private final Migration MigrateAddSecretsIndex = new Migration()
    {
        @Override
        public String getVersion()
        {
            return "20160830123456";
        }

        @Override
        public void migrate(Handle handle)
        {
            handle.update("create unique index secrets_on_site_id_and_project_id_and_scope_and_key on secrets (site_id, project_id, scope, key)");
        }
    };

    private final Migration[] migrations = {
        MigrateCreateTables,
        MigrateSessionsOnProjectIdIndexToDesc,
        MigrateCreateResumingTasks,
        MigrateMakeProjectsDeletable,
        MigrateAddUserInfoColumnToRevisions,
        MigrateQueueRearchitecture,
        MigrateAddSecretsTable,
        MigrateAddFinishedAtToSessionAttempts,
        MigrateQueueUniqueName,
        MigrateAddSecretsIndex,
    };
}
