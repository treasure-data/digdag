package io.digdag.standards.task.td;

import java.util.List;
import java.io.File;
import java.io.PrintStream;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskRunner;
import io.digdag.spi.TaskRunnerFactory;
import io.digdag.standards.task.BaseTaskRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import org.msgpack.value.Value;

public class TdTaskRunnerFactory
        implements TaskRunnerFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdTaskRunnerFactory.class);

    @Inject
    public TdTaskRunnerFactory()
    { }

    public String getType()
    {
        return "td";
    }

    @Override
    public TaskRunner newTaskExecutor(TaskRequest request)
    {
        return new TdTaskRunner(request);
    }

    private class TdTaskRunner
            extends BaseTaskRunner
    {
        public TdTaskRunner(TaskRequest request)
        {
            super(request);
        }

        @Override
        public Config runTask()
        {
            Config config = request.getConfig();

            String command = config.get("command", String.class);

            Optional<String> insertInto = config.getOptional("insert_into", String.class);
            Optional<String> createTable = config.getOptional("create_table", String.class);
            if (insertInto.isPresent() && createTable.isPresent()) {
                throw new ConfigException("Setting both insert_into and create_table is invalid");
            }

            int priority = config.get("priority", int.class, 0);
            Optional<String> resultUrl = config.getOptional("result_url", String.class);

            int jobRetry = config.get("job_retry", int.class, 0);

            String engine = config.get("engine", String.class, "presto");

            Optional<String> downloadFile = config.getOptional("download_file", String.class);

            try (TDOperation op = TDOperation.fromConfig(config)) {
                String query;

                switch(engine) {
                case "presto":
                    if (insertInto.isPresent()) {
                        op.ensureTableCreated(insertInto.get());
                        query = "INSERT INTO " + op.escapeIdentPresto(insertInto.get()) + "\n" + command;
                    }
                    else if (createTable.isPresent()) {
                        op.ensureTableDeleted(createTable.get());
                        query = "CREATE TABLE " + op.escapeIdentPresto(createTable.get()) + " AS\n" + command;
                    }
                    else {
                        query = command;
                    }
                    break;

                case "hive":
                    if (insertInto.isPresent()) {
                        op.ensureTableCreated(createTable.get());
                        query = "INSERT INTO TABLE " + op.escapeIdentHive(insertInto.get()) + "\n" + command;
                    }
                    else if (createTable.isPresent()) {
                        op.ensureTableCreated(createTable.get());
                        query = "INSERT INTO OVERWRITE " + op.escapeIdentHive(createTable.get()) + " AS\n" + command;
                    }
                    else {
                        query = command;
                    }
                    break;

                default:
                    throw new ConfigException("Unknown 'engine:' option (available options are: hive and presto): "+engine);
                }

                TDJobRequest req = new TDJobRequestBuilder()
                    .setResultOutput(resultUrl.orNull())
                    .setType(engine)
                    .setDatabase(op.getDatabase())
                    .setQuery(query)
                    .setRetryLimit(jobRetry)
                    .setPriority(priority)
                    .createTDJobRequest();

                TDQuery q = new TDQuery(op.getClient(), req);
                logger.info("Started {} query id={}:\n{}", q.getJobId(), engine, query);

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

                return request.getConfig().getFactory().create()
                    .set("td_job_id", q.getJobId());
            }
        }
    }
}
