package io.digdag.standards.operator.redshift;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.ConfigException;
import io.digdag.standards.operator.pg.PgConnection;
import io.digdag.standards.operator.jdbc.TransactionHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

import com.google.common.annotations.VisibleForTesting;
import io.digdag.standards.operator.jdbc.LockConflictException;
import io.digdag.standards.operator.jdbc.DatabaseException;
import org.postgresql.core.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Locale.ENGLISH;

public class RedshiftConnection
    extends PgConnection
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @VisibleForTesting
    public static RedshiftConnection open(RedshiftConnectionConfig config)
    {
        return new RedshiftConnection(config.openConnection());
    }

    RedshiftConnection(Connection connection)
    {
        super(connection);
    }

    @Override
    public TransactionHelper getStrictTransactionHelper(String statusTableName, Duration cleanupDuration)
    {
        return new RedshiftPersistentTransactionHelper(statusTableName, cleanupDuration);
    }

    private static String escapeParam(String param)
    {
        StringBuilder sb = new StringBuilder();
        try {
            return Utils.escapeLiteral(sb, param, false).toString();
        }
        catch (SQLException e) {
            throw new ConfigException("Failed to escape a parameter in configuration file: param=" + param, e);
        }
    }

    private <T> void appendOption(StringBuilder sb, String name, Optional<T> param)
    {
        appendOption(sb, name, param, false);
    }

    private <T> void appendOption(StringBuilder sb, String name, Optional<T> param, boolean withoutEscape)
    {
        if (!param.isPresent()) {
            return;
        }

        T v = param.get();
        if (v instanceof Boolean && !((Boolean)v)) {
            return;
        }

        sb.append(name);

        if (v instanceof String) {
            String escape = withoutEscape ? "" : "'";
            String s = (String) v;
            if (!s.isEmpty()) {
                sb.append(String.format(" %s%s%s", escape, escapeParam(s), escape));
            }
        }
        else if (v instanceof Number) {
            Number n = (Number) v;
            sb.append(String.format(" %d", n));
        }
        sb.append("\n");
    }

    String buildCopyStatement(CopyConfig copyConfig)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(
                String.format("COPY %s FROM '%s'\n",
                        escapeIdent(copyConfig.tableName),
                        escapeParam(copyConfig.from)));

        // credentials
        sb.append(
                // TODO: Use session token
                String.format("CREDENTIALS '%s'\n",
                        escapeParam(
                                String.format("aws_access_key_id=%s;aws_secret_access_key=%s",
                                        copyConfig.accessKeyId, copyConfig.secretAccessKey))));

        appendOption(sb, "READRATIO", copyConfig.readratio);
        appendOption(sb, "MANIFEST", copyConfig.manifest);
        appendOption(sb, "ENCRYPTED", copyConfig.encrypted);
        appendOption(sb, "REGION", copyConfig.region);

        // Data Format Parameters
        if (copyConfig.csv.isPresent()) {
            // Syntax around CSV is a bit inconsistent...
            String s = copyConfig.csv.get();
            sb.append("CSV");
            if (!s.isEmpty()) {
                sb.append(String.format(" QUOTE '%s'", escapeParam(s)));
            }
            sb.append("\n");
        }
        appendOption(sb, "DELIMITER", copyConfig.delimiter);
        appendOption(sb, "FIXEDWIDTH", copyConfig.fixedwidth);
        appendOption(sb, "FORMAT AS JSON", copyConfig.json);
        appendOption(sb, "FORMAT AS AVRO", copyConfig.avro);

        // Data Conversion Parameters
        appendOption(sb, "ACCEPTANYDATE", copyConfig.acceptanydate);
        appendOption(sb, "ACCEPTINVCHARS", copyConfig.acceptinvchars);
        appendOption(sb, "BLANKSASNULL", copyConfig.blanksasnull);
        appendOption(sb, "DATEFORMAT", copyConfig.dateformat);
        appendOption(sb, "EMPTYASNULL", copyConfig.emptyasnull);
        appendOption(sb, "ENCODING", copyConfig.encoding, true);
        appendOption(sb, "ESCAPE", copyConfig.escape);
        appendOption(sb, "EXPLICIT_IDS", copyConfig.explicitIds);
        appendOption(sb, "FILLRECORD", copyConfig.fillrecord);
        appendOption(sb, "IGNOREBLANKLINES", copyConfig.ignoreblanklines);
        appendOption(sb, "IGNOREHEADER", copyConfig.ignoreheader);
        appendOption(sb, "NULL AS", copyConfig.nullAs);
        appendOption(sb, "REMOVEQUOTES", copyConfig.removequotes);
        appendOption(sb, "ROUNDEC", copyConfig.roundec);
        appendOption(sb, "TIMEFORMAT", copyConfig.timeformat);
        appendOption(sb, "TRIMBLANKS", copyConfig.trimblanks);
        appendOption(sb, "TRUNCATECOLUMNS", copyConfig.truncatecolumns);

        // Data Load Operations
        appendOption(sb, "COMPROWS", copyConfig.comprows);
        appendOption(sb, "COMPUPDATE", copyConfig.compupdate, true);
        appendOption(sb, "MAXERROR", copyConfig.maxerror);
        appendOption(sb, "NOLOAD", copyConfig.noload);
        appendOption(sb, "STATUPDATE", copyConfig.statupdate, true);

        return sb.toString();
    }

    private class RedshiftPersistentTransactionHelper
            extends PgPersistentTransactionHelper
    {
        RedshiftPersistentTransactionHelper(String statusTableName, Duration cleanupDuration)
        {
            super(statusTableName, cleanupDuration);
        }

        @Override
        public void cleanup()
        {
            try (Statement stmt = connection.createStatement()) {
                // List up status tables
                List<String> statusTables = new ArrayList<>();
                {
                    ResultSet rs = stmt.executeQuery(
                            String.format(ENGLISH,
                                    "SELECT tablename FROM pg_tables WHERE tablename LIKE '%s_%%'",
                                    escapeParam(statusTableName))
                    );
                    while (rs.next()) {
                        statusTables.add(rs.getString(1));
                    }
                }

                // Drop a status table if it is expired
                statusTables.forEach(
                        statusTable -> {
                            try {
                                ResultSet rs = stmt.executeQuery(
                                        String.format(ENGLISH,
                                                "SELECT query_id FROM %s WHERE created_at IS NOT NULL AND completed_at < CURRENT_TIMESTAMP - INTERVAL '%d SECOND'",
                                                escapeIdent(statusTable),
                                                cleanupDuration.getSeconds())
                                );
                                if (rs.next()) {
                                    stmt.executeUpdate(String.format("DROP TABLE %s", escapeIdent(statusTable)));
                                }
                            }
                            catch (SQLException e) {
                                logger.warn("Failed to drop expired status table: {}. Ignoring...", statusTable, e);
                            }
                        }
                );
            }
            catch (SQLException ex) {
                throw new DatabaseException("Failed to list up expired status tables", ex);
            }
        }

        @Override
        protected String statusTableName(UUID queryId)
        {
            return String.format("%s_%s", statusTableName, queryId);
        }

        @Override
        protected String buildCreateTable(UUID queryId)
        {
            // Redshift doesn't support timestamptz type. Timestamp type is always UTC.
            return String.format(ENGLISH,
                    "CREATE TABLE IF NOT EXISTS %s" +
                            " (query_id text NOT NULL UNIQUE, created_at timestamp NOT NULL, completed_at timestamp)",
                    escapeIdent(statusTableName(queryId)));
        }

        @Override
        protected StatusRow lockStatusRow(UUID queryId)
                throws LockConflictException
        {
            try (Statement stmt = connection.createStatement()) {
                String escapedStatusTableName = escapeIdent(statusTableName(queryId));

                stmt.executeUpdate(String.format(ENGLISH, "LOCK TABLE %s", escapedStatusTableName));

                ResultSet rs = stmt.executeQuery(
                        String.format(ENGLISH, "SELECT completed_at FROM %s WHERE query_id = '%s'",
                                escapedStatusTableName, queryId.toString())
                );
                if (rs.next()) {
                    // status row exists and locked. get status of it.
                    rs.getTimestamp(1);
                    if (rs.wasNull()) {
                        return StatusRow.LOCKED_NOT_COMPLETED;
                    }
                    else {
                        return StatusRow.LOCKED_COMPLETED;
                    }
                }
                else {
                    return StatusRow.NOT_EXISTS;
                }
            }
            catch (SQLException ex) {
                // Redshift doesn't support "LOCK TABLE NOWAIT",
                // so "55P03 lock_not_available" shouldn't happen here
                throw new DatabaseException("Failed to lock a status row", ex);
            }
        }
    }

    @FunctionalInterface
    interface CopyConfigConfigurator
    {
        void config(CopyConfig orig);
    }

    static class CopyConfig
    {
        String accessKeyId;
        String secretAccessKey;
        String sessionToken;

        String tableName;
        Optional<String> columnList = Optional.absent();
        String from;
        Optional<Integer> readratio = Optional.absent();
        Optional<Boolean> manifest = Optional.absent();
        Optional<Boolean> encrypted = Optional.absent();
        Optional<String> region = Optional.absent();

        Optional<String> csv = Optional.absent();
        Optional<String> delimiter = Optional.absent();
        Optional<String> fixedwidth = Optional.absent();
        Optional<String> json = Optional.absent();
        Optional<String> avro = Optional.absent();
        Optional<Boolean> gzip = Optional.absent();
        Optional<Boolean> bzip2 = Optional.absent();
        Optional<Boolean> lzop = Optional.absent();

        Optional<Boolean> acceptanydate = Optional.absent();
        Optional<String> acceptinvchars = Optional.absent();
        Optional<Boolean> blanksasnull = Optional.absent();
        Optional<String> dateformat = Optional.absent();
        Optional<Boolean> emptyasnull = Optional.absent();
        Optional<String> encoding = Optional.absent();
        Optional<Boolean> escape = Optional.absent();
        Optional<Boolean> explicitIds = Optional.absent();
        Optional<Boolean> fillrecord = Optional.absent();
        Optional<Boolean> ignoreblanklines = Optional.absent();
        Optional<Integer> ignoreheader = Optional.absent();
        Optional<String> nullAs = Optional.absent();
        Optional<Boolean> removequotes = Optional.absent();
        Optional<Boolean> roundec = Optional.absent();
        Optional<String> timeformat = Optional.absent();
        Optional<Boolean> trimblanks = Optional.absent();
        Optional<Boolean> truncatecolumns = Optional.absent();
        Optional<Integer> comprows = Optional.absent();
        Optional<String> compupdate = Optional.absent();
        Optional<Integer> maxerror = Optional.absent();
        Optional<Boolean> noload = Optional.absent();
        Optional<String> statupdate = Optional.absent();

        private CopyConfig()
        {
        }

        private static final List<String> ACCEPTED_FLAGS_FOR_XXXDATE = ImmutableList.of("ON", "OFF", "TRUE", "FALSE");

        private void validate()
        {
            if (accessKeyId == null || secretAccessKey == null) {
                throw new ConfigException("'accessKeyId' or 'secretAccessKey' shouldn't be null");
            }

            if (tableName == null || from == null) {
                throw new ConfigException("'tableName' or 'from' shouldn't be null");
            }

            if (csv.isPresent()) {
                if (fixedwidth.isPresent() || removequotes.or(false) || escape.or(false)) {
                    throw new ConfigException("CSV cannot be used with FIXEDWIDTH, REMOVEQUOTES, or ESCAPE");
                }
            }
            if (delimiter.isPresent()) {
                if (fixedwidth.isPresent()) {
                    throw new ConfigException("DELIMITER cannot be used with FIXEDWIDTH");
                }
            }
            if (json.isPresent()) {
                if (csv.isPresent() || delimiter.isPresent() || escape.or(false) || fillrecord.or(false)
                    || avro.isPresent() || fixedwidth.isPresent() || ignoreblanklines.or(false) || nullAs.isPresent()
                    || readratio.isPresent() || removequotes.or(false)) {
                    throw new ConfigException("JSON cannot be used with CSV, DELIMITER, AVRO, ESCAPE, FILLRECORD, FIXEDWIDTH, IGNOREBLANKLINES, NULL AS, READRATIO or REMOVEQUOTES");
                }
            }
            if (avro.isPresent()) {
                // As for AVRO, the documents doesn't mention any limitation. So we left the minimum validations here
                if (csv.isPresent() || delimiter.isPresent() || json.isPresent() || fixedwidth.isPresent()) {
                    throw new ConfigException("AVRO cannot be used with CSV, DELIMITER, JSON or FIXEDWIDTH");
                }
            }
            // As for FIXEDWIDTH, combinations with other format are already validated

            if (statupdate.isPresent()) {
                if (!ACCEPTED_FLAGS_FOR_XXXDATE.contains(statupdate.get().toUpperCase())) {
                    throw new ConfigException("STATUPDATE should be in ON/OFF/TRUE/FALSE: " + statupdate.get());
                }
            }

            if (compupdate.isPresent()) {
                if (!ACCEPTED_FLAGS_FOR_XXXDATE.contains(compupdate.get().toUpperCase())) {
                    throw new ConfigException("COMPUPDATE should be in ON/OFF/TRUE/FALSE: " + compupdate.get());
                }
            }
        }

        public static CopyConfig configure(CopyConfigConfigurator configurator)
        {
            CopyConfig copyConfig = new CopyConfig();
            configurator.config(copyConfig);
            copyConfig.validate();
            return copyConfig;
        }
    }
}
