package io.digdag.standards.operator.td;

import java.util.Date;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.format.DateTimeFormatter;
import com.google.inject.Inject;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.standards.operator.BaseOperator;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDExportJobRequest;
import com.treasuredata.client.model.TDExportFileFormatType;
import org.msgpack.value.Value;
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
    public Operator newTaskExecutor(Path archivePath, TaskRequest request)
    {
        return new TdTableExportOperator(archivePath, request);
    }

    private class TdTableExportOperator
            extends BaseOperator
    {
        public TdTableExportOperator(Path archivePath, TaskRequest request)
        {
            super(archivePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().setAllIfNotSet(
                    request.getConfig().getNestedOrGetEmpty("td"));

            String fileFormatString = params.get("file_format", String.class);
            TDExportFileFormatType fileFormat;
            try {
                fileFormat = TDExportFileFormatType.fromName(fileFormatString);
            }
            catch (RuntimeException ex) {
                throw new ConfigException("invalid file_format option", ex);
            }

            TDExportJobRequest req = new TDExportJobRequest(
                    params.get("database", String.class),
                    params.get("table", String.class),
                    Date.from(parseTime(params, "from")),
                    Date.from(parseTime(params, "to")),
                    fileFormat,
                    params.get("s3_access_key_id", String.class),
                    params.get("s3_secret_access_key", String.class),
                    params.get("s3_bucket", String.class),
                    params.get("s3_path_prefix", String.class),
                    params.getOptional("pool_name", String.class));

            try (TDOperation op = TDOperation.fromConfig(params)) {
                TDQuery q = new TDQuery(op.getClient(), op.getClient().submitExportJob(req));
                logger.info("Started table export job id={}", q.getJobId());

                try {
                    q.ensureSucceeded();
                }
                catch (InterruptedException ex) {
                    Throwables.propagate(ex);
                }
                finally {
                    q.ensureFinishedOrKill();
                }

                return TaskResult.empty(request);
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
