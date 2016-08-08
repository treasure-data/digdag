package io.digdag.standards.operator.td;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.nio.file.Path;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.StringWriter;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.annotations.VisibleForTesting;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TaskExecutionException;
import io.digdag.util.BaseOperator;
import io.digdag.util.Workspace;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.treasuredata.client.TDClientHttpNotFoundException;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import org.msgpack.value.Value;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.RawValue;
import org.msgpack.value.ValueFactory;

import static io.digdag.standards.operator.td.TDOperator.escapeHiveTableName;
import static io.digdag.standards.operator.td.TDOperator.escapePrestoTableName;
import static java.nio.charset.StandardCharsets.UTF_8;
import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static io.digdag.standards.operator.td.TDOperator.escapeHiveIdent;
import static io.digdag.standards.operator.td.TDOperator.escapePrestoIdent;

public class TdOperatorFactory
        implements OperatorFactory
{
    private static final String JOB_ID = "jobId";
    private static final String DOMAIN_KEY = "domainKey";
    private static final String POLL_ITERATION = "pollIteration";

    private static final Integer INITIAL_POLL_INTERVAL = 1;
    private static final int MAX_POLL_INTERVAL = 30000;

    private static Logger logger = LoggerFactory.getLogger(TdOperatorFactory.class);

    private static final int PREVIEW_ROWS = 20;

    private final TemplateEngine templateEngine;

    @Inject
    public TdOperatorFactory(TemplateEngine templateEngine)
    {
        this.templateEngine = templateEngine;
    }

    public String getType()
    {
        return "td";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new TdOperator(workspacePath, request);
    }

    private class TdOperator
            extends BaseOperator
    {
        private final Config params;
        private final String query;
        private final Optional<TableParam> insertInto;
        private final Optional<TableParam> createTable;
        private final int priority;
        private final Optional<String> resultUrl;
        private final int jobRetry;
        private final String engine;
        private final Optional<String> downloadFile;
        private final boolean storeLastResults;
        private final boolean preview;
        private final Config state;
        private final Optional<String> existingJobId;
        private final Optional<String> existingDomainKey;

        public TdOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);

            this.params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            this.query = templateEngine.templateCommand(workspacePath, params, "query", UTF_8);

            this.insertInto = params.getOptional("insert_into", TableParam.class);
            this.createTable = params.getOptional("create_table", TableParam.class);
            if (insertInto.isPresent() && createTable.isPresent()) {
                throw new ConfigException("Setting both insert_into and create_table is invalid");
            }

            this.priority = params.get("priority", int.class, 0);  // TODO this should accept string (VERY_LOW, LOW, NORMAL, HIGH VERY_HIGH)
            this.resultUrl = params.getOptional("result_url", String.class);

            this.jobRetry = params.get("job_retry", int.class, 0);

            this.engine = params.get("engine", String.class, "presto");

            this.downloadFile = params.getOptional("download_file", String.class);
            if (downloadFile.isPresent() && (insertInto.isPresent() || createTable.isPresent())) {
                // query results become empty if INSERT INTO or CREATE TABLE query runs
                throw new ConfigException("download_file is invalid if insert_into or create_table is set");
            }

            this.storeLastResults = params.get("store_last_results", boolean.class, false);

            this.preview = params.get("preview", boolean.class, false);

            this.state = request.getLastStateParams().deepCopy();

            this.existingJobId = state.getOptional(JOB_ID, String.class);
            this.existingDomainKey = state.getOptional(DOMAIN_KEY, String.class);
        }

        @Override
        public TaskResult runTask()
        {
            try (TDOperator op = TDOperator.fromConfig(params)) {

                // Generate and store domain key before starting the job
                if (!existingDomainKey.isPresent()) {
                    String domainKey = UUID.randomUUID().toString();
                    state.set(DOMAIN_KEY, domainKey);
                    throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
                }

                // Start the job
                if (!existingJobId.isPresent()) {
                    String jobId = startJob(op);
                    state.set(JOB_ID, jobId);
                    state.set(POLL_ITERATION, 1);
                    throw TaskExecutionException.ofNextPolling(INITIAL_POLL_INTERVAL, ConfigElement.copyOf(state));
                }

                // Check if the job is done
                String jobId = existingJobId.get();
                TDJobOperator job = op.newJobOperator(jobId);
                TDJobSummary status = checkJobStatus(job);
                boolean done = status.getStatus().isFinished();
                if (!done) {
                    int pollIteration = state.get(POLL_ITERATION, int.class, 1);
                    state.set(POLL_ITERATION, pollIteration + 1);
                    int pollInterval = (int) Math.min(INITIAL_POLL_INTERVAL * Math.pow(2, pollIteration), MAX_POLL_INTERVAL);
                    throw TaskExecutionException.ofNextPolling(pollInterval, ConfigElement.copyOf(state));
                }

                // Get the job results
                return processJobResult(op, job, status);
            }
        }

        private TaskResult processJobResult(TDOperator op, TDJobOperator j, TDJobSummary summary)
        {
            downloadJobResult(j, workspace, downloadFile);

            if (preview) {
                try {
                    if (insertInto.isPresent() || createTable.isPresent()) {
                        selectPreviewRows(op, "job id " + j.getJobId(),
                                insertInto.isPresent() ? insertInto.get() : createTable.get());
                    }
                    else {
                        downloadPreviewRows(j, "job id " + j.getJobId());
                    }
                }
                catch (Exception ex) {
                    logger.info("Getting rows for preview failed. Ignoring this error.", ex);
                }
            }

            Config storeParams = buildStoreParams(request.getConfig().getFactory(), j, summary, storeLastResults);

            return TaskResult.defaultBuilder(request)
                    .storeParams(storeParams)
                    .build();
        }

        private String startJob(TDOperator op)
        {
            String stmt;
            switch(engine) {
                case "presto":
                    if (insertInto.isPresent()) {
                        ensureTableCreated(op, insertInto.get());
                        stmt = insertCommandStatement("INSERT INTO " + escapePrestoTableName(insertInto.get()), query);
                    }
                    else if (createTable.isPresent()) {
                        // TODO: this is not atomic
                        String tableName = escapePrestoTableName(createTable.get());
                        stmt = insertCommandStatement(
                                "DROP TABLE IF EXISTS " + tableName + ";\n" +
                                        "CREATE TABLE " + tableName + " AS", query);
                    }
                    else {
                        stmt = query;
                    }
                    break;

                case "hive":
                    if (insertInto.isPresent()) {
                        ensureTableCreated(op, insertInto.get());
                        stmt = insertCommandStatement("INSERT INTO TABLE " + escapeHiveTableName(insertInto.get()), query);
                    }
                    else if (createTable.isPresent()) {
                        ensureTableCreated(op, createTable.get());
                        stmt = insertCommandStatement("INSERT OVERWRITE TABLE " + escapeHiveTableName(createTable.get()), query);
                    }
                    else {
                        stmt = query;
                    }
                    break;

                default:
                    throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): "+engine);
            }

            assert existingDomainKey.isPresent();
            TDJobRequest req = new TDJobRequestBuilder()
                    .setResultOutput(resultUrl.orNull())
                    .setType(engine)
                    .setDatabase(op.getDatabase())
                    .setQuery(stmt)
                    .setRetryLimit(jobRetry)
                    .setPriority(priority)
                    .setScheduledTime(request.getSessionTime().getEpochSecond())
                    .setDomainKey(existingDomainKey.get())
                    .createTDJobRequest();

            TDJobOperator j = op.submitNewJobWithRetry(req);
            logger.info("Started {} job id={}:\n{}", engine, j.getJobId(), stmt);

            return j.getJobId();
        }

        private TDJobSummary checkJobStatus(TDJobOperator j)
        {
            try {
                return j.ensureRunningOrSucceeded();
            }
            catch (TDJobException ex) {
                try {
                    TDJob job = j.getJobInfo();
                    String message = job.getCmdOut() + "\n" + job.getStdErr();
                    throw new TaskExecutionException(message, buildExceptionErrorConfig(ex));
                }
                catch (Exception getJobInfoFailed) {
                    getJobInfoFailed.addSuppressed(ex);
                    throw Throwables.propagate(getJobInfoFailed);
                }
            }
            catch (InterruptedException ex) {
                throw Throwables.propagate(ex);
            }
        }
    }

    static void selectPreviewRows(TDOperator op, String description, TableParam destTable)
    {
        String comment = "-- preview results of " + description;

        TDJobRequest req = new TDJobRequestBuilder()
            .setType("presto")
            .setDatabase(op.getDatabase())
            .setQuery(comment + "\nSELECT * FROM " + escapePrestoTableName(destTable) + " LIMIT 20")
            .setPriority(0)
            .createTDJobRequest();

        TDJobOperator j = op.submitNewJob(req);
        downloadPreviewRows(j, "table " + destTable.toString());
    }

    static void downloadPreviewRows(TDJobOperator j, String description)
    {
        StringWriter out = new StringWriter();

        try {
            addCsvHeader(out, j.getResultColumnNames());

            List<ArrayValue> rows = downloadFirstResults(j, PREVIEW_ROWS);
            if (rows.isEmpty()) {
                logger.info("preview of {}: (no results)", description, j.getJobId());
                return;
            }
            for (ArrayValue row : rows) {
                addCsvRow(out, row);
            }
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }

        logger.info("preview of {}:\r\n{}", description, out.toString());
    }

    private final static Pattern INSERT_LINE_PATTERN = Pattern.compile("(\\A|\\r?\\n)\\-\\-\\s*DIGDAG_INSERT_LINE(?:(?!\\n|\\z).)*", Pattern.MULTILINE);

    private final static Pattern HEADER_COMMENT_BLOCK_PATTERN = Pattern.compile("\\A([\\r\\n\\t]*(?:(?:\\A|\\n)\\-\\-[^\\n]*)+)\\n?(.*)\\z", Pattern.MULTILINE);

    @VisibleForTesting
    static String insertCommandStatement(String command, String original)
    {
        // try to insert command at "-- DIGDAG_INSERT_LINE" line
        Matcher ml = INSERT_LINE_PATTERN.matcher(original);
        if (ml.find()) {
            return ml.replaceFirst(ml.group(1) + command);
        }

        // try to insert command after header comments so that job list page
        // shows comments rather than INSERT or other non-informative commands
        Matcher mc = HEADER_COMMENT_BLOCK_PATTERN.matcher(original);
        if (mc.find()) {
            return mc.group(1) + "\n" + command + "\n" + mc.group(2);
        }

        // add command at the head
        return command + "\n" + original;
    }

    private static void ensureTableDeleted(TDOperator op, TableParam table)
    {
        op.withDatabase(table.getDatabase().or(op.getDatabase())).ensureTableDeleted(table.getTable());
    }

    private static void ensureTableCreated(TDOperator op, TableParam table)
    {
        op.withDatabase(table.getDatabase().or(op.getDatabase())).ensureTableCreated(table.getTable());
    }

    private static String escapeHiveTableName(TableParam table)
    {
        if (table.getDatabase().isPresent()) {
            return escapeHiveIdent(table.getDatabase().get()) + '.' + escapeHiveIdent(table.getTable());
        }
        else {
            return escapeHiveIdent(table.getTable());
        }
    }

    private static String escapePrestoTableName(TableParam table)
    {
        if (table.getDatabase().isPresent()) {
            return escapePrestoIdent(table.getDatabase().get()) + '.' + escapePrestoIdent(table.getTable());
        }
        else {
            return escapePrestoIdent(table.getTable());
        }
    }

    static void downloadJobResult(TDJobOperator j, Workspace workspace, Optional<String> downloadFile)
    {
        if (downloadFile.isPresent()) {
            j.getResult(ite -> {
                try (BufferedWriter out = workspace.newBufferedWriter(downloadFile.get(), UTF_8)) {
                    addCsvHeader(out, j.getResultColumnNames());

                    while (ite.hasNext()) {
                        addCsvRow(out, ite.next().asArrayValue());
                    }
                    return true;
                }
                catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
            });
        }
    }

    private static void addCsvHeader(Writer out, List<String> columnNames)
        throws IOException
    {
        boolean first = true;
        for (String columnName : columnNames) {
            if (first) { first = false; }
            else { out.write(DELIMITER_CHAR); }
            addCsvText(out, columnName);
        }
        out.write("\r\n");
    }

    private static void addCsvRow(Writer out, ArrayValue row)
        throws IOException
    {
        boolean first = true;
        for (Value v : row) {
            if (first) { first = false; }
            else { out.write(DELIMITER_CHAR); }
            addCsvValue(out, v);
        }
        out.write("\r\n");
    }

    static Config buildStoreParams(ConfigFactory cf, TDJobOperator j, TDJobSummary summary, boolean storeLastResults)
    {
        Config td = cf.create();

        td.set("last_job_id", summary.getJobId());

        if (storeLastResults) {
            List<ArrayValue> results = downloadFirstResults(j, 1);
            ArrayValue row = results.get(0);
            Map<RawValue, Value> map = new LinkedHashMap<>();
            List<String> columnNames = j.getResultColumnNames();
            for (int i=0; i < Math.min(results.size(), columnNames.size()); i++) {
                map.put(ValueFactory.newString(columnNames.get(i)), row.get(i));
            }
            MapValue lastResults = ValueFactory.newMap(map);
            try {
                td.set("last_results", new ObjectMapper().readTree(lastResults.toJson()));
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        return cf.create().set("td", td);
    }

    private static List<ArrayValue> downloadFirstResults(TDJobOperator j, int max)
    {
        List<ArrayValue> results = new ArrayList<ArrayValue>(max);
        try {
            j.getResult(ite -> {
                for (int i=0; i < max; i++) {
                    if (ite.hasNext()) {
                        ArrayValue row = ite.next().asArrayValue();
                        results.add(row);
                    }
                    else {
                        break;
                    }
                }
                return true;
            });
        }
        catch (TDClientHttpNotFoundException ex) {
            // this happens if query is INSERT or CREATE. return empty results
        }
        return results;
    }

    private static void addCsvValue(Writer out, Value value)
        throws IOException
    {
        if (value.isStringValue()) {
            addCsvText(out, value.asStringValue().asString());
        }
        else if (value.isNilValue()) {
            // write nothing
        }
        else {
            addCsvText(out, value.toJson());
        }
    }

    private static void addCsvText(Writer out, String value)
        throws IOException
    {
        out.write(escapeAndQuoteCsvValue(value));
    }

    private static final char DELIMITER_CHAR = ',';
    private static final char ESCAPE_CHAR = '"';
    private static final char QUOTE_CHAR = '"';

    private static String escapeAndQuoteCsvValue(String v)
    {
        if (v.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append(QUOTE_CHAR);
            sb.append(QUOTE_CHAR);
            return sb.toString();
        }

        StringBuilder escapedValue = new StringBuilder();
        char previousChar = ' ';

        boolean isRequireQuote = false;

        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);

            if (c == QUOTE_CHAR) {
                escapedValue.append(ESCAPE_CHAR);
                escapedValue.append(c);
                isRequireQuote = true;
            }
            else if (c == '\r') {
                escapedValue.append('\n');
                isRequireQuote = true;
            }
            else if (c == '\n') {
                if (previousChar != '\r') {
                    escapedValue.append('\n');
                    isRequireQuote = true;
                }
            }
            else if (c == DELIMITER_CHAR) {
                escapedValue.append(c);
                isRequireQuote = true;
            }
            else {
                escapedValue.append(c);
            }
            previousChar = c;
        }

        if (isRequireQuote) {
            StringBuilder sb = new StringBuilder();
            sb.append(QUOTE_CHAR);
            sb.append(escapedValue);
            sb.append(QUOTE_CHAR);
            return sb.toString();
        }
        else {
            return escapedValue.toString();
        }
    }
}
