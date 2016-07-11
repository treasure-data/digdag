package io.digdag.standards.operator.td;

import java.util.Date;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.util.BaseOperator;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import com.treasuredata.client.model.TDExportJobRequest;
import com.treasuredata.client.model.TDExportFileFormatType;

import static java.util.Locale.ENGLISH;

public class TdTableExportOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdTableExportOperatorFactory.class);

    @Inject
    public TdTableExportOperatorFactory()
    { }

    public String getType()
    {
        return "td_table_export";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new TdTableExportOperator(workspacePath, request);
    }

    private class TdTableExportOperator
            extends BaseOperator
    {
        public TdTableExportOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            String fileFormatString = params.get("file_format", String.class);
            TDExportFileFormatType fileFormat;
            try {
                fileFormat = TDExportFileFormatType.fromName(fileFormatString);
            }
            catch (RuntimeException ex) {
                throw new ConfigException("invalid file_format option", ex);
            }

            String database = params.get("database", String.class);
            TableParam table = params.get("table", TableParam.class);

            TDExportJobRequest req = new TDExportJobRequest(
                    table.getDatabase().or(database),
                    table.getTable(),
                    Date.from(parseTime(params, "from")),
                    Date.from(parseTime(params, "to")),
                    fileFormat,
                    params.get("s3_access_key_id", String.class),
                    params.get("s3_secret_access_key", String.class),
                    params.get("s3_bucket", String.class),
                    params.get("s3_path_prefix", String.class),
                    params.getOptional("pool_name", String.class));

            try (TDOperator op = TDOperator.fromConfig(params)) {
                TDJobOperator j = op.submitExportJob(req);
                logger.info("Started table export job id={}", j.getJobId());

                j.joinJob();

                Config storeParams = request.getConfig().getFactory().create()
                    .set("td", request.getConfig().getFactory().create()
                            .set("last_job_id", j.getJobId()));

                return TaskResult.defaultBuilder(request)
                    .storeParams(storeParams)
                    .build();
            }
        }
    }

    private static final DateTimeFormatter TIME_PARSER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[ Z]", ENGLISH);

    private static Instant parseTime(Config params, String key)
    {
        try {
            return Instant.ofEpochSecond(
                    params.get(key, long.class)
                    );
        }
        catch (ConfigException ex) {
            return Instant.from(
                    TIME_PARSER
                    .withZone(params.get("timezone", ZoneId.class))
                    .parse(params.get(key, String.class))
                    );
        }
    }
}
