package io.digdag.core;

import java.util.List;
import java.util.ArrayList;
import com.google.inject.Inject;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Handle;

public class DatabaseMigrator
{
    private final IDBI dbi;

    @Inject
    public DatabaseMigrator(IDBI dbi)
    {
        this.dbi = dbi;
    }

    private static class CreateTableBuilder
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
            handle.update("create unique index sessions_on_site_id_and_unique_name on sessions (site_id, unique_name)");
        }
    }
}
