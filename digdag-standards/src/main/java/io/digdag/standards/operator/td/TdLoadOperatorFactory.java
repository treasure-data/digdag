package io.digdag.standards.operator.td;

import java.nio.file.Path;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.Throwables;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import io.digdag.standards.operator.BaseOperator;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import org.msgpack.value.Value;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TdLoadOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdLoadOperatorFactory.class);

    private final TemplateEngine templateEngine;

    @Inject
    public TdLoadOperatorFactory(TemplateEngine templateEngine)
    {
        this.templateEngine = templateEngine;
    }

    public String getType()
    {
        return "td_load";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new TdLoadOperator(workspacePath, request);
    }

    private class TdLoadOperator
            extends BaseOperator
    {
        public TdLoadOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            ObjectNode embulkConfig;
            if (params.has("_command")) {
                String built;
                String command = params.get("_command", String.class);
                try {
                    built = templateEngine.templateFile(workspacePath, command, UTF_8, params);
                }
                catch (IOException | TemplateException ex) {
                    throw new ConfigException("Failed to load bulk load file", ex);
                }

                try {
                    embulkConfig = new YamlLoader().loadString(built);
                }
                catch (RuntimeException | IOException ex) {
                    Throwables.propagateIfInstanceOf(ex, ConfigException.class);
                    throw new ConfigException("Failed to parse yaml file", ex);
                }
            }
            else {
                embulkConfig = params.getNested("config").getInternalObjectNode();
            }

            TableParam table = params.get("table", TableParam.class);

            try (TDOperator op = TDOperator.fromConfig(params)) {
                TDJobRequest req = TDJobRequest
                    .newBulkLoad(table.getDatabase().or(op.getDatabase()), table.getTable(), embulkConfig);

                TDJobOperator j = op.submitNewJob(req);
                logger.info("Started bulk load job id={}", j.getJobId());

                try {
                    j.ensureSucceeded();
                }
                catch (InterruptedException ex) {
                    Throwables.propagate(ex);
                }
                finally {
                    j.ensureFinishedOrKill();
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
}
