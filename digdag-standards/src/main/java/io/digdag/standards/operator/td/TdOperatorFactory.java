package io.digdag.standards.operator.td;

import java.util.List;
import java.nio.file.Path;
import java.io.File;
import java.io.PrintStream;
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
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import org.msgpack.value.Value;
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
            Config params = request.getConfig().getNestedOrGetEmpty("td")
                .deepCopy()
                .setAll(request.getConfig());

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

            try (TDOperation op = TDOperation.fromConfig(params)) {
                String stmt;

                switch(engine) {
                case "presto":
                    if (insertInto.isPresent()) {
                        op.ensureTableCreated(insertInto.get());
                        stmt = "INSERT INTO " + op.escapeIdentPresto(insertInto.get()) + "\n" + query;
                    }
                    else if (createTable.isPresent()) {
                        op.ensureTableDeleted(createTable.get());
                        stmt = "CREATE TABLE " + op.escapeIdentPresto(createTable.get()) + " AS\n" + query;
                    }
                    else {
                        stmt = query;
                    }
                    break;

                case "hive":
                    if (insertInto.isPresent()) {
                        op.ensureTableCreated(createTable.get());
                        stmt = "INSERT INTO TABLE " + op.escapeIdentHive(insertInto.get()) + "\n" + query;
                    }
                    else if (createTable.isPresent()) {
                        op.ensureTableCreated(createTable.get());
                        stmt = "INSERT INTO OVERWRITE " + op.escapeIdentHive(createTable.get()) + " AS\n" + query;
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

                TDQuery q = new TDQuery(op.getClient(), req);
                logger.info("Started {} job id={}:\n{}", q.getJobId(), engine, stmt);

                try {
                    q.ensureSucceeded();
                }
                catch (RuntimeException ex) {
                    try {
                        TDJob job = q.getJobInfo();
                        String message = job.getCmdOut() + "\n" + job.getStdErr();
                        logger.warn("Job {}:\n===\n{}\n===", q.getJobId(), message);
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
                    q.ensureFinishedOrKill();
                }

                if (downloadFile.isPresent()) {
                    try (PrintStream out = new PrintStream(new File(downloadFile.get()))) {
                        q.getResult(ite -> {
                            // TODO get schema from q.getJobInfo().getResultSchema()
                            while (ite.hasNext()) {
                                Value v = ite.next();
                                // TODO save as csv
                                out.println(v.toString());
                            }
                            return true;
                        });
                    }
                    catch (IOException ex) {
                        throw Throwables.propagate(ex);
                    }
                }

                Config exportParams = request.getConfig().getFactory().create();

                Config storeParams = request.getConfig().getFactory().create()
                    .getNestedOrSetEmpty("td")
                    .set("last_job_id", q.getJobId());

                return TaskResult.defaultBuilder(request)
                    .exportParams(exportParams)
                    .storeParams(storeParams)
                    .build();
            }
        }
    }
}
