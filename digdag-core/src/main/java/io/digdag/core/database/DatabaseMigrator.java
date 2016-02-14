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
    public DatabaseMigrator(IDBI dbi, DatabaseConfig config)
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

        public CreateTableBuilder addIntIdNoAutoIncrement(String column, String options)
        {
            return add(column, "int primary key AUTO_INCREMENT " + options);
        }

        public CreateTableBuilder addLongId(String column)
        {
            return add(column, "bigint primary key AUTO_INCREMENT");
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

        public CreateTableBuilder addString(String column, String options)
        {
            if (databaseType.equals("postgresql")) {
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
            if (databaseType.equals("postgresql")) {
                return add(column, "bytea " + options);
            }
            else {
                return add(column, "varbinary(255) " + options);
            }
        }

        public CreateTableBuilder addLongBinary(String column, String options)
        {
            return add(column, "blob " + options);
        }

        public CreateTableBuilder addTimestamp(String column, String options)
        {
            if (databaseType.equals("postgresql")) {
                return add(column, "timestamp with time zone " + options);
            }
            else {
                return add(column, "timestamp " + options);
            }
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
                    .addTimestamp("created_at", "not null")
                    //.addTimestamp("deleted_at", "not null")  // this points UNIXTIME 0 (1970-01-01 00:00:00 UTC) if this repository is not deleted
                    .build());
            handle.update("create unique index if not exists repositories_on_site_id_and_name on repositories (site_id, name)");
            //handle.update("create unique index if not exists repositories_on_site_id_and_name on repositories (site_id, name, deleted_at)");

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
            handle.update("create unique index if not exists revisions_on_repository_id_and_name on revisions (repository_id, name)");
            handle.update("create index if not exists revisions_on_repository_id_and_id on revisions (repository_id, id)");

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
            handle.update("create index if not exists workflow_configs_on_repository_id_and_config_digest on workflow_configs (repository_id, config_digest)");

            // workflow_definitions
            handle.update(
                    new CreateTableBuilder("workflow_definitions")
                    .addLongId("id")
                    .addInt("config_id", "not null references workflow_configs (id)")
                    .addInt("revision_id", "not null references revisions (id)")
                    .addString("name", "not null")
                    .build());
            handle.update("create unique index if not exists workflow_definitions_on_revision_id_and_name on workflow_definitions (revision_id, name)");

            // schedules
            handle.update(
                    new CreateTableBuilder("schedules")
                    .addIntId("id")
                    .addInt("repository_id", "not null references repositories (id)")
                    .addLong("workflow_definition_id", "not null references workflow_definitions (id)")
                    .addLong("next_run_time", "not null")
                    .addLong("next_schedule_time", "not null")
                    .addLong("last_session_time", "")
                    .addTimestamp("created_at", "not null")
                    .addTimestamp("updated_at", "not null")
                    .build());
            handle.update("create index if not exists schedules_on_repository_id on schedules (repository_id)");
            handle.update("create unique index if not exists schedules_on_workflow_definition_id on schedules (workflow_definition_id)");
            handle.update("create index if not exists schedules_on_next_run_time on schedules (next_run_time)");

            // sessions
            handle.update(
                    new CreateTableBuilder("sessions")
                    .addLongId("id")
                    .addInt("repository_id", "not null references repositories (id)")
                    .addString("workflow_name", "not null")
                    .addLong("session_time", "not null")
                    .addLong("last_attempt_id", "")
                    .build());
            handle.update("create unique index if not exists sessions_on_repository_id_and_workflow_name_and_session_time on sessions (repository_id, workflow_name, session_time)");
            handle.update("create index if not exists sessions_on_repository_id on sessions (repository_id, id)");
            handle.update("create index if not exists sessions_on_repository_id_and_workflow_name on sessions (repository_id, workflow_name, id)");

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
            handle.update("create unique index if not exists session_attempts_on_session_id_and_attempt_name on session_attempts (session_id, attempt_name)");
            handle.update("create index if not exists session_attempts_on_site_id on session_attempts (site_id, id)");
            handle.update("create index if not exists session_attempts_on_workflow_definition_id on session_attempts (workflow_definition_id, id)");
            handle.update("create index if not exists session_attempts_on_repository_id on session_attempts (repository_id, id)");

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
            handle.update("create index if not exists session_monitors_on_attempt_id on session_monitors (attempt_id)");
            handle.update("create index if not exists session_monitors_on_next_run_time on session_monitors (next_run_time)");

            // tasks
            handle.update(
                    new CreateTableBuilder("tasks")
                    .addLongId("id")
                    .addLong("attempt_id", "not null references session_attempts (id)")
                    .addLong("parent_id", "references tasks (id)")
                    .addShort("task_type", "")   // 0=action, 1=grouping  NOT NULL
                    //.addShort("error_mode", "")  // 1=ignore_parent_flags  NOT NULL
                    .addShort("state", "not null")
                    .addShort("state_flags", "not null")
                    .addTimestamp("retry_at", "")
                    .addTimestamp("updated_at", "not null")  // last state update is done at this time
                    .build());
            handle.update("create index if not exists tasks_on_attempt_id on tasks (attempt_id, id)");
            handle.update("create index if not exists tasks_on_parent_id on tasks (parent_id)");

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
                    .addMediumText("state_params", "")
                    .addMediumText("carry_params", "")
                    .addMediumText("error", "")
                    .addMediumText("report", "")
                    .build());

            // task_dependencies
            handle.update(
                    new CreateTableBuilder("task_dependencies")
                    .addLongId("id")
                    .addLong("upstream_id", "not null")
                    .addLong("downstream_id", "not null")
                    .build());
            handle.update("create index if not exists task_dependencies_on_downstream_id on task_dependencies (downstream_id)");

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
            handle.update("create unique index if not exists queue_settings_on_site_id_and_name on queue_settings (site_id, name)");
            handle.update("create index if not exists queue_settings_on_site_id on queue_settings (site_id, id)");

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
            handle.update("create unique index if not exists resource_types_on_queue_id_and_name on resource_types (queue_id, name)");

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
            handle.update("create unique index if not exists queued_tasks_on_queue_id_task_id on queued_tasks (queue_id, task_id)");

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

            switch (databaseType) {
            case "postgresql":
                handle.update("create index if not exists queued_shared_task_locks_grouping on queued_shared_tasks (queue_id, resource_type_id) where hold_expire_time is not null");
                handle.update("create index if not exists queued_shared_task_locks_ordering on queued_shared_tasks (queue_id, priority desc, id) where hold_expire_time is null");
                handle.update("create index if not exists queued_shared_task_locks_expiration on queued_shared_tasks (hold_expire_time) where hold_expire_time is not null");
                handle.update("create index if not exists queued_task_locks_grouping on queued_task_locks (queue_id, resource_type_id) where hold_expire_time is not null");
                handle.update("create index if not exists queued_task_locks_ordering on queued_task_locks (queue_id, priority desc, id) where hold_expire_time is null");
                handle.update("create index if not exists queued_task_locks_expiration on queued_task_locks (hold_expire_time) where hold_expire_time is not null");
                // explain analyze verbose
                // with rrs as (
                //   select resource_type_id, count(*) as count
                //   from queued_task_locks
                //   where queue_id = 1
                //   and hold_expire_time is not null
                //   and resource_type_id is not null
                //   group by resource_type_id
                // ),
                // resource_exceeds as (
                //   select rt.id
                //   from resource_types rt
                //   left join rrs on rt.id = rrs.resource_type_id
                //   where rt.max_concurrency <= coalesce(count, 0)
                // )
                // select ks.id
                // from queued_task_locks ks
                // where queue_id = (
                //   select 1 as id
                //   from queues
                //   where id = 1
                //   and max_concurrency > (
                //     select sum(count) as count
                //     from rrs
                //   )
                // )
                // and not exists (
                //   select * from resource_exceeds
                //   where resource_exceeds.id = ks.resource_type_id
                // )
                // and hold_expire_time is null
                // order by priority desc, id asc
                // limit 1
            default:
                handle.update("create index queued_shared_task_locks_grouping on queued_shared_task_locks (hold_expire_time, queue_id, resource_type_id)");
                handle.update("create index queued_shared_task_locks_ordering on queued_shared_task_locks (queue_id, hold_expire_time, priority desc, id)");
                handle.update("create index queued_task_locks_grouping on queued_task_locks (hold_expire_time, queue_id, resource_type_id)");
                handle.update("create index queued_task_locks_ordering on queued_task_locks (queue_id, hold_expire_time, priority desc, id)");
                // select ks.id
                // from task_locks ks
                // left join (
                //     select rt.id
                //     from resource_types rt
                //     left join (
                //         select resource_type_id, count(*) as count
                //         from task_locks
                //         where queue_id = 1
                //         and hold_expire_time is not null
                //         and resource_type_id is not null
                //         group by resource_type_id
                //     ) rr on rt.id = rr.resource_type_id
                //     where rt.max_concurrency <= coalesce(count, 0)
                // ) rc on ks.resource_type_id = rc.id
                // where queue_id = (
                //     select 1 from queues
                //     where id = 1
                //     and max_concurrency > (
                //         select count(*) as count
                //         from task_locks
                //         where queue_id = 1
                //         and hold_expire_time is not null
                //     )
                // )
                // and hold_expire_time is null
                // and rc.id is null
                // order by priority desc, id asc
                // limit 1
            }
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
