package io.digdag.core;

import java.util.List;
import java.util.ArrayList;
import com.google.inject.Inject;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Handle;

public class DatabaseMigrator
{
    private final IDBI dbi;
    private final String databaseType;

    @Inject
    public DatabaseMigrator(IDBI dbi, DatabaseStoreConfig config)
    {
        this.dbi = dbi;
        this.databaseType = config.getType();
    }

    public static String getDriverClassName(String type)
    {
        switch (type) {
        case "h2":
            return "org.h2.Driver";
        case "sqlite":
            return "org.sqlite.JDBC";
        default:
            throw new RuntimeException("Unsupported database type: "+type);
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
            if (databaseType.equals("sqlite")) {
                return add(column, "integer primary key AUTOINCREMENT");
            }
            else {
                return add(column, "int primary key AUTO_INCREMENT");
            }
        }

        public CreateTableBuilder addLongId(String column)
        {
            if (databaseType.equals("sqlite")) {
                return add(column, "integer primary key AUTOINCREMENT");
            }
            else {
                return add(column, "bigint primary key AUTO_INCREMENT");
            }
        }

        public CreateTableBuilder addInt(String column, String options)
        {
            return add(column, "int " + options);
        }

        public CreateTableBuilder addString(String column, String options)
        {
            return add(column, "varchar(255) " + options);
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

    public void migrate()
    {
        // TODO references
        try (Handle handle = dbi.open()) {
            // sessions
            handle.update(
                    new CreateTableBuilder("sessions")
                    .addIntId("id")
                    .addInt("site_id", "not null")
                    .addInt("workflow_id", "")  // nullable if one-time workflow
                    .addString("unique_name", "not null")
                    .addLongText("session_params", "not null")
                    //.addLongText("session_options", "not null")
                    .addTimestamp("created_at", "not null")
                    .build());
            handle.update("create unique index if not exists sessions_on_site_id_and_unique_name on sessions (site_id, unique_name)");

            // repositories
            handle.update(
                    new CreateTableBuilder("repositories")
                    .addIntId("id")
                    .addInt("site_id", "not null")
                    .addString("name", "not null")
                    .addLongText("config", "not null")
                    //.addBoolean("disabled", "not null")
                    //.addInt("latest_revision_id", "")
                    .addTimestamp("created_at", "not null")
                    .addTimestamp("updated_at", "not null")
                    .build());
            handle.update("create unique index if not exists repositories_on_site_id_and_name on repositories (site_id, name)");

            // revisions
            handle.update(
                    new CreateTableBuilder("revisions")
                    .addIntId("id")
                    // TODO disabled flag
                    .addInt("repository_id", "not null")
                    .addString("name", "not null")
                    .addLongText("global_params", "not null")
                    .addString("archive_type", "not null")
                    .addBinary("archive_md5", "")
                    .addString("archive_path", "")
                    .addLongBinary("archive_data", "")
                    .addTimestamp("created_at", "not null")
                    .build());
            handle.update("create unique index if not exists revisions_on_repository_id_and_name on revisions (repository_id, name)");

            // workflows
            handle.update(
                    new CreateTableBuilder("workflows")
                    .addIntId("id")
                    .addInt("revision_id", "not null")
                    .addString("name", "not null")
                    .addLongText("config", "not null")
                    .build());
            handle.update("create unique index if not exists workflows_on_revision_id_and_name on workflows (revision_id, name)");
        }
    }
}
