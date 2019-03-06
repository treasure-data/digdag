package io.digdag.standards.operator.redshift;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.ConfigException;
import io.digdag.standards.operator.jdbc.TableReference;
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

import static com.google.common.base.Preconditions.checkNotNull;
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
    public TransactionHelper getStrictTransactionHelper(String statusTableSchema, String statusTableName, Duration cleanupDuration)
    {
        return new RedshiftPersistentTransactionHelper(statusTableSchema, statusTableName, cleanupDuration);
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

    private void appendCredentialsPart(StringBuilder sb, StatementConfig config, boolean maskCredentials)
    {
        String credentials;
        String accessKeyId;
        String secretAccessKey;
        if (maskCredentials) {
            accessKeyId = "********";
            secretAccessKey = "********";
        }
        else {
            accessKeyId = config.accessKeyId;
            secretAccessKey = config.secretAccessKey;
        }

        if (config.sessionToken.isPresent()) {
            String sessionToken;
            if (maskCredentials) {
                sessionToken = "********";
            }
            else {
                sessionToken = (String) config.sessionToken.get();
            }
            credentials = String.format("aws_access_key_id=%s;aws_secret_access_key=%s;token=%s",
                            accessKeyId, secretAccessKey, sessionToken);
        }
        else {
            credentials = String.format("aws_access_key_id=%s;aws_secret_access_key=%s",
                    accessKeyId, secretAccessKey);
        }
        sb.append(String.format("CREDENTIALS '%s'\n", escapeParam(credentials)));
    }


    String buildCopyStatement(CopyConfig copyConfig, boolean maskCredentials)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(
                String.format("COPY %s FROM '%s'\n",
                        escapeIdent(copyConfig.table),
                        escapeParam(copyConfig.from)));

        // credentials
        appendCredentialsPart(sb, copyConfig, maskCredentials);

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
        appendOption(sb, "GZIP", copyConfig.gzip);
        appendOption(sb, "BZIP2", copyConfig.bzip2);
        appendOption(sb, "LZOP", copyConfig.lzop);

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

    String buildUnloadStatement(UnloadConfig unloadConfig, boolean maskCredentials)
    {
        StringBuilder sb = new StringBuilder();

        sb.append(
                String.format("UNLOAD ('%s') TO '%s'\n",
                        escapeParam(unloadConfig.query),
                        escapeParam(unloadConfig.toWithPrefixDir)));

        // credentials
        appendCredentialsPart(sb, unloadConfig, maskCredentials);

        // Options
        appendOption(sb, "MANIFEST", unloadConfig.manifest);
        appendOption(sb, "ENCRYPTED", unloadConfig.encrypted);
        appendOption(sb, "DELIMITER", unloadConfig.delimiter);
        appendOption(sb, "FIXEDWIDTH", unloadConfig.fixedwidth);
        appendOption(sb, "GZIP", unloadConfig.gzip);
        appendOption(sb, "BZIP2", unloadConfig.bzip2);
        appendOption(sb, "ADDQUOTES", unloadConfig.addquotes);
        appendOption(sb, "NULL AS", unloadConfig.nullAs);
        appendOption(sb, "ESCAPE", unloadConfig.escape);
        appendOption(sb, "ALLOWOVERWRITE", unloadConfig.allowoverwrite);
        appendOption(sb, "PARALLEL", unloadConfig.parallel, true);

        return sb.toString();
    }

    // Redshift doesn't support row-level locks or FOR UPDATE NOWAIT. Therefore, unlike
    // PgPersistentTransactionHelper, here uses following strategy:
    //
    //   1. BEGIN transaction
    //   2. CREATE TABLE ${status_table}_${queryId} (query_id, created_at, completed_at)
    //      AS SELECT '${queryId}'::text, SYSDATE::timestamptz, NULL::timestamptz";
    //   3. If CREATE TABLE succeeded, this transaction is locking the table. Run the action,
    //      and COMMIT the transaction.
    //   4. If CREATE TABLE failed with SQL state 23505, another thread or node succeeded to
    //      commit CREATE TABLE statement before. Thus skip the action.
    //
    private class RedshiftPersistentTransactionHelper
            implements TransactionHelper
    {
        private String statusTableSchema;
        private String statusTableNamePrefix;
        private final Duration cleanupDuration;

        RedshiftPersistentTransactionHelper(String statusTableSchema, String statusTableNamePrefix, Duration cleanupDuration)
        {
            this.statusTableSchema = statusTableSchema;
            this.statusTableNamePrefix = statusTableNamePrefix;
            this.cleanupDuration = cleanupDuration;
        }

        private TableReference statusTableReference(UUID queryId)
        {
            String tableName = String.format("%s_%s", statusTableNamePrefix, queryId);
            if (statusTableSchema != null) {
                return TableReference.of(statusTableSchema, tableName);
            }
            else {
                return TableReference.of(tableName);
            }
        }

        @Override
        public void prepare(UUID queryId)
        {
            // do nothing
        }

        @Override
        public boolean lockedTransaction(UUID queryId, TransactionAction action)
                throws LockConflictException
        {
            beginTransaction();
            boolean created = createLockedTableWithStatusRow(queryId);

            if (created) {
                // CREATE TABLE AS succeeded in this transaction.
                // No other threads / nodes has succeeded to commit before.
                // This transaction is responsible to run the action.
                action.run();
                updateStatusRowAndCommitTransaction(queryId);
                return true;
            }
            else {
                // CREATE TABLE AS conflicted.
                // A thread / node succeeded to commit before.
                // Skip the action.
                abortTransaction();
                return false;
            }
        }

        private void beginTransaction()
        {
            executeStatement("begin a transaction", "BEGIN");
        }

        private void abortTransaction()
        {
            executeStatement("rollback a transaction", "ROLLBACK");
        }

        private void updateStatusRowAndCommitTransaction(UUID queryId)
        {
            executeStatement("update status row",
                    String.format(ENGLISH,
                        "UPDATE %s SET completed_at = SYSDATE WHERE query_id = '%s'",
                        escapeTableReference(statusTableReference(queryId)),
                        queryId.toString())
                    );
            executeStatement("commit updated status row", "COMMIT");
        }

        private void executeStatement(String desc, String sql)
        {
            try {
                execute(sql);
            }
            catch (SQLException ex) {
                throw new DatabaseException("Failed to " + desc, ex);
            }
        }

        private boolean createLockedTableWithStatusRow(UUID queryId)
        {
            String sql = String.format(ENGLISH,
                    "CREATE TABLE %s" +
                    " (query_id, created_at, completed_at)" +
                    " AS SELECT '%s'::text, SYSDATE::timestamptz, NULL::timestamptz",
                    escapeTableReference(statusTableReference(queryId)),
                    queryId.toString());
            try {
                execute(sql);
                return true;
            }
            catch (SQLException ex) {
                if (isConflictException(ex)) {
                    abortTransaction();
                    return false;
                }
                else {
                    String desc = "Failed to create a status table.\n"
                            + "hint: if you don't have permission to create tables, "
                            + "please try one of these options:\n"
                            + "1. add 'strict_transaction: false' option to disable "
                            + "exactly-once transaction control that depends on this table.\n"
                            + "2. ask system administrator to create a schema that this user can create a table "
                            + "and set 'status_table_schema' option to it\n";
                    throw new DatabaseException(desc, ex);
                }
            }
        }

        boolean isConflictException(SQLException ex)
        {
            return "23505".equals(ex.getSQLState());
        }

        @Override
        public void cleanup()
        {
            try (Statement stmt = connection.createStatement()) {
                // List up status tables
                List<TableReference> statusTables = new ArrayList<>();
                {
                    String sql = String.format(ENGLISH,
                            "SELECT schemaname, tablename FROM pg_tables WHERE tablename LIKE '%s_%%'",
                            escapeParam(statusTableNamePrefix));
                    ResultSet rs = executeQuery(stmt,sql);
                    while (rs.next()) {
                        statusTables.add(TableReference.of(rs.getString(1), rs.getString(2)));
                    }
                }

                // Drop a status table if its completed_at is older than cleanupDuration
                statusTables.forEach(
                        statusTable -> {
                            try {
                                String sql = String.format(ENGLISH,
                                        "SELECT query_id FROM %s WHERE completed_at < SYSDATE - INTERVAL '%d SECOND'",
                                        escapeTableReference(statusTable),
                                        cleanupDuration.getSeconds());
                                ResultSet rs = executeQuery(stmt,sql);
                                if (rs.next()) {
                                    sql = String.format("DROP TABLE %s", escapeTableReference(statusTable));
                                    executeUpdate(sql);
                                }
                            }
                            catch (SQLException e) {
                                logger.warn("Failed to drop expired status table: {}. Ignoring this error. To not show this warning message, please confirm that this user has privilege to DROP tables whose name is prefixed with '{}_'", statusTable, statusTableNamePrefix, e);
                            }
                        }
                );
            }
            catch (SQLException ex) {
                throw new DatabaseException("Failed to list up expired status tables", ex);
            }
        }
    }

    @FunctionalInterface
    interface ConfigConfigurator<T>
    {
        void config(T orig);
    }

    public abstract static class StatementConfig<T extends StatementConfig>
    {
        static final List<String> ACCEPTED_FLAGS = ImmutableList.of("ON", "OFF", "TRUE", "FALSE");

        String accessKeyId;
        String secretAccessKey;
        Optional<String> sessionToken = Optional.absent();

        void validate()
        {
            if (accessKeyId == null || secretAccessKey == null) {
                throw new ConfigException("'accessKeyId' or 'secretAccessKey' shouldn't be null");
            }

            validateInternal();
        }

        abstract void validateInternal();

        public void configure(ConfigConfigurator<T> configurator)
        {
            @SuppressWarnings("unchecked")
            T casted = (T) this;
            configurator.config(casted);
            validate();
        }
    }

    static class CopyConfig
            extends StatementConfig<CopyConfig>
    {
        String table;
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

        @Override
        void validateInternal()
        {
            if (table == null) {
                throw new ConfigException("'table' shouldn't be null");
            }

            if (from == null) {
                throw new ConfigException("'from' shouldn't be null");
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
                if (!ACCEPTED_FLAGS.contains(statupdate.get().toUpperCase())) {
                    throw new ConfigException("STATUPDATE should be in ON/OFF/TRUE/FALSE: " + statupdate.get());
                }
            }
            if (compupdate.isPresent()) {
                if (!ACCEPTED_FLAGS.contains(compupdate.get().toUpperCase())) {
                    throw new ConfigException("COMPUPDATE should be in ON/OFF/TRUE/FALSE: " + compupdate.get());
                }
            }
        }
    }

    static class UnloadConfig
            extends StatementConfig<UnloadConfig>
    {
        String query;
        String to;
        String toWithPrefixDir;
        String s3Bucket;
        String s3Prefix;
        Optional<String> parallel = Optional.absent();
        Optional<Boolean> manifest = Optional.absent();
        Optional<Boolean> allowoverwrite = Optional.absent();
        Optional<Boolean> encrypted = Optional.absent();

        Optional<String> delimiter = Optional.absent();
        Optional<String> fixedwidth = Optional.absent();
        Optional<Boolean> gzip = Optional.absent();
        Optional<Boolean> bzip2 = Optional.absent();
        Optional<String> nullAs = Optional.absent();
        Optional<Boolean> escape = Optional.absent();
        Optional<Boolean> addquotes = Optional.absent();

        @Override
        void validateInternal()
        {
            if (query == null) {
                throw new ConfigException("'query' shouldn't be null");
            }
            if (to == null) {
                throw new ConfigException("'to' shouldn't be null");
            }
            if (parallel.isPresent()) {
                if (!ACCEPTED_FLAGS.contains(parallel.get().toUpperCase())) {
                    throw new ConfigException("PARALLEL should be in ON/OFF/TRUE/FALSE: " + parallel.get());
                }
            }
        }

        void setupWithPrefixDir(String prefixDir)
        {
            checkNotNull(prefixDir);

            StringBuilder sb = new StringBuilder(to);
            if (!to.endsWith("/")) {
                sb.append("/");
            }
            sb.append(prefixDir);
            sb.append("_");
            toWithPrefixDir = sb.toString();

            String head = "s3://";
            if (!toWithPrefixDir.startsWith(head)) {
                throw new ConfigException("'to' should start with '" + head + "'. to=" + to);
            }
            int slashAfterBucket = toWithPrefixDir.indexOf("/", head.length());
            if (slashAfterBucket < 0) {
                throw new ConfigException("'to' should include a bucket name and key. to=" + to);
            }
            s3Bucket = toWithPrefixDir.substring(head.length(), slashAfterBucket);
            if (s3Bucket.length() == 0) {
                throw new ConfigException("'to' includes empty bucket. to=" + to);
            }
            s3Prefix = toWithPrefixDir.substring(slashAfterBucket + 1);
            if (s3Prefix.length() == 0) {
                throw new ConfigException("'to' includes empty prefix key. to=" + to);
            }
        }
    }
}
