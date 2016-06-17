package io.digdag.standards.operator.td;

import java.nio.file.Path;
import java.io.IOException;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.common.base.Throwables;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDBulkLoadSessionStartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import io.digdag.util.BaseOperator;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import com.treasuredata.client.model.TDJobRequest;

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

            Optional<Config> config = params.getOptionalNested("config");
            Optional<String> name = params.getOptional("name", String.class);
            Optional<String> command = params.getOptional("_command", String.class);

            if (config.isPresent() && name.isPresent()) {
                throw new ConfigException("The parameters config and name cannot both be set");
            }

            ObjectNode embulkConfig;
            if (command.isPresent()) {
                if (command.get().endsWith(".yml") || command.get().endsWith(".yaml")) {
                    embulkConfig = compileEmbulkConfig(params, command.get());
                    return executeBulkLoad(params, embulkConfig);
                } else {
                    return executeBulkLoadSession(params, command.get(), request);
                }
            }
            else if (name.isPresent()) {
                return executeBulkLoadSession(params, name.get(), request);
            }
            else if (config.isPresent()) {
                embulkConfig = config.get().getInternalObjectNode();
                return executeBulkLoad(params, embulkConfig);
            } else {
                throw new ConfigException("The parameters config and name cannot both be set");
            }
        }

        private TaskResult executeBulkLoadSession(Config params, String name, TaskRequest request)
        {
            try (TDClient client = TDClientFactory.clientFromConfig(params)) {

                TDBulkLoadSessionStartResult r =
                        client.startBulkLoadSession(name, request.getSessionTime().getEpochSecond());

                logger.info("Started bulk load session job name={}, id={}", name, r.getJobId());

                TDJobOperator jobOperator = new TDJobOperator(client, r.getJobId());

                return join(jobOperator);
            }
        }

        private TaskResult executeBulkLoad(Config params, ObjectNode embulkConfig)
        {
            TableParam table = params.get("table", TableParam.class);

            try (TDOperator op = TDOperator.fromConfig(params)) {
                TDJobRequest req = TDJobRequest
                    .newBulkLoad(table.getDatabase().or(op.getDatabase()), table.getTable(), embulkConfig);

                TDJobOperator j = op.submitNewJob(req);
                logger.info("Started bulk load job id={}", j.getJobId());

                return join(j);
            }
        }

        private TaskResult join(TDJobOperator j)
        {
            j.joinJob();

            Config storeParams = request.getConfig().getFactory().create()
                .set("td", request.getConfig().getFactory().create()
                        .set("last_job_id", j.getJobId()));

            return TaskResult.defaultBuilder(request)
                .storeParams(storeParams)
                .build();
        }

        private ObjectNode compileEmbulkConfig(Config params, String command)
        {
            ObjectNode embulkConfig;
            String built;
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
            return embulkConfig;
        }
    }
}
