package io.digdag.standards.operator.td;

import java.util.List;
import java.nio.file.Path;
import java.io.File;
import java.io.BufferedWriter;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import io.digdag.standards.operator.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import org.msgpack.value.Value;
import org.msgpack.value.ArrayValue;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TdOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdOperatorFactory.class);

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
    public Operator newTaskExecutor(Path archivePath, TaskRequest request)
    {
        return new TdOperator(archivePath, request);
    }

    private class TdOperator
            extends BaseOperator
    {
        public TdOperator(Path archivePath, TaskRequest request)
        {
            super(archivePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            String query = templateEngine.templateCommand(archivePath, params, "query", UTF_8);

            Optional<String> insertInto = params.getOptional("insert_into", String.class);
            Optional<String> createTable = params.getOptional("create_table", String.class);
            if (insertInto.isPresent() && createTable.isPresent()) {
                throw new ConfigException("Setting both insert_into and create_table is invalid");
            }

            int priority = params.get("priority", int.class, 0);  // TODO this should accept string (VERY_LOW, LOW, NORMAL, HIGH VERY_HIGH)
            Optional<String> resultUrl = params.getOptional("result_url", String.class);

            int jobRetry = params.get("job_retry", int.class, 0);

            String engine = params.get("engine", String.class, "presto");

            Optional<String> downloadFile = params.getOptional("download_file", String.class);
            if (downloadFile.isPresent() && (insertInto.isPresent() || createTable.isPresent())) {
                // query results become empty if INSERT INTO or CREATE TABLE query runs
                throw new ConfigException("download_file is invalid if insert_into or create_table is set");
            }

            try (TDOperator op = TDOperator.fromConfig(params)) {
                String stmt;

                switch(engine) {
                case "presto":
                    if (insertInto.isPresent()) {
                        op.ensureTableCreated(insertInto.get());
                        stmt = "INSERT INTO " + op.escapePrestoIdent(insertInto.get()) + "\n" + query;
                    }
                    else if (createTable.isPresent()) {
                        op.ensureTableDeleted(createTable.get());
                        stmt = "CREATE TABLE " + op.escapePrestoIdent(createTable.get()) + " AS\n" + query;
                    }
                    else {
                        stmt = query;
                    }
                    break;

                case "hive":
                    if (insertInto.isPresent()) {
                        op.ensureTableCreated(createTable.get());
                        stmt = "INSERT INTO TABLE " + op.escapeHiveIdent(insertInto.get()) + "\n" + query;
                    }
                    else if (createTable.isPresent()) {
                        op.ensureTableCreated(createTable.get());
                        stmt = "INSERT INTO OVERWRITE " + op.escapeHiveIdent(createTable.get()) + " AS\n" + query;
                    }
                    else {
                        stmt = query;
                    }
                    break;

                default:
                    throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): "+engine);
                }

                TDJobRequest req = new TDJobRequestBuilder()
                    .setResultOutput(resultUrl.orNull())
                    .setType(engine)
                    .setDatabase(op.getDatabase())
                    .setQuery(stmt)
                    .setRetryLimit(jobRetry)
                    .setPriority(priority)
                    .createTDJobRequest();

                TDJobOperator j = op.submitNewJob(req);
                logger.info("Started {} job id={}:\n{}", j.getJobId(), engine, stmt);

                try {
                    j.ensureSucceeded();
                }
                catch (RuntimeException ex) {
                    try {
                        TDJob job = j.getJobInfo();
                        String message = job.getCmdOut() + "\n" + job.getStdErr();
                        logger.warn("Job {}:\n===\n{}\n===", j.getJobId(), message);
                    }
                    catch (Throwable fail) {
                        ex.addSuppressed(fail);
                    }
                    throw ex;
                }
                catch (InterruptedException ex) {
                    Throwables.propagate(ex);
                }
                finally {
                    j.ensureFinishedOrKill();
                }

                if (downloadFile.isPresent()) {
                    j.getResult(ite -> {
                        try (BufferedWriter out = archive.newBufferedWriter(downloadFile.get(), UTF_8)) {
                            boolean firstCol = true;
                            for (String col : j.getResultColumnNames()) {
                                if (firstCol) { firstCol = false; }
                                else { out.write(DELIMITER_CHAR); }
                                addCsvText(out, col);
                            }
                            out.write("\n");

                            try {
                                while (ite.hasNext()) {
                                    ArrayValue row = ite.next().asArrayValue();
                                    boolean first = true;
                                    for (Value v : row) {
                                        if (first) { first = false; }
                                        else { out.write(DELIMITER_CHAR); }
                                        addCsvValue(out, v);
                                    }
                                    out.write("\n");
                                }
                                return true;
                            }
                            catch (IOException ex) {
                                throw Throwables.propagate(ex);
                            }
                        }
                        catch (IOException ex) {
                            throw Throwables.propagate(ex);
                        }
                    });
                }

                Config storeParams = request.getConfig().getFactory().create()
                    .set("td", request.getConfig().getFactory().create()
                            .set("last_job_id", j.getJobId()));

                return TaskResult.defaultBuilder(request)
                    .storeParams(storeParams)
                    .build();
            }
        }
    }

    private static void addCsvValue(BufferedWriter out, Value value)
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

    private static void addCsvText(BufferedWriter out, String value)
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
