package io.digdag.standards.operator.td;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.treasuredata.client.model.TDBulkLoadSessionStartRequest;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobRequestBuilder;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TdLoadOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(TdLoadOperatorFactory.class);

    private final TemplateEngine templateEngine;
    private final Map<String, String> env;
    private final Config systemConfig;

    @Inject
    public TdLoadOperatorFactory(TemplateEngine templateEngine, @Environment Map<String, String> env, Config systemConfig)
    {
        this.templateEngine = templateEngine;
        this.env = env;
        this.systemConfig = systemConfig;
    }

    public String getType()
    {
        return "td_load";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new TdLoadOperator(projectPath, request);
    }

    private class TdLoadOperator
            extends BaseTdJobOperator
    {
        private final Config params;
        private final Optional<Config> config;
        private final Optional<String> name;
        private final Optional<String> command;

        private final Optional<String> sessionName;
        private final Optional<ObjectNode> embulkConfig;

        protected TdLoadOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request, env, systemConfig);

            params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            config = params.getOptionalNested("config");
            name = params.getOptional("name", String.class);
            command = params.getOptional("_command", String.class);

            if (config.isPresent() && name.isPresent()) {
                throw new ConfigException("The parameters config and name cannot both be set");
            }

            if (Stream.of(command, name, config).filter(Optional::isPresent).count() > 1) {
                throw new ConfigException("Only the command or one of the config and name params may be set");
            }

            if (command.isPresent()) {
                if (command.get().endsWith(".yml") || command.get().endsWith(".yaml")) {
                    this.embulkConfig = Optional.of(compileEmbulkConfig(params, command.get()));
                    this.sessionName = Optional.absent();
                }
                else {
                    this.embulkConfig = Optional.absent();
                    this.sessionName = Optional.of(command.get());
                }
            }
            else if (name.isPresent()) {
                this.embulkConfig = Optional.absent();
                this.sessionName = name;
            }
            else if (config.isPresent()) {
                this.embulkConfig = Optional.of(config.get().getInternalObjectNode());
                this.sessionName = Optional.absent();
            }
            else {
                throw new AssertionError();
            }
        }

        @Override
        protected String startJob(TaskExecutionContext ctx, TDOperator op, String domainKey)
        {
            assert Stream.of(embulkConfig, sessionName).filter(Optional::isPresent).count() == 1;

            if (embulkConfig.isPresent()) {
                return startBulkLoad(op, params, embulkConfig.get(), domainKey);
            }
            else if (sessionName.isPresent()) {
                return startBulkLoadSession(op, params, sessionName.get(), request, domainKey);
            }
            else {
                throw new AssertionError();
            }
        }

        private String startBulkLoadSession(TDOperator op, Config params, String name, TaskRequest request, String domainKey)
        {
            // TODO: TDOperator requires database to be configured but the database param is not necessary when using a connector session

            TDBulkLoadSessionStartRequest req = TDBulkLoadSessionStartRequest.builder()
                    .setScheduledTime(request.getSessionTime().getEpochSecond())
                    .setDomainKey(domainKey)
                    .build();

            String jobId = op.submitNewJobWithRetry(client -> client.startBulkLoadSession(name, req).getJobId());

            logger.info("Started bulk load session job name={}, id={}", name, jobId);

            return jobId;
        }

        private String startBulkLoad(TDOperator op, Config params, ObjectNode embulkConfig, String domainKey)
        {
            TableParam table = params.get("table", TableParam.class);

            TDJobRequest req = new TDJobRequestBuilder()
                    .setType(TDJob.Type.BULKLOAD)
                    .setDatabase(table.getDatabase().or(op.getDatabase()))
                    .setTable(table.getTable())
                    .setConfig(embulkConfig)
                    .setQuery("")
                    .setDomainKey(domainKey)
                    .createTDJobRequest();

            String jobId = op.submitNewJobWithRetry(req);
            logger.info("Started bulk load job id={}", jobId);

            return jobId;
        }

        private ObjectNode compileEmbulkConfig(Config params, String command)
        {
            ObjectNode embulkConfig;
            String built;
            try {
                built = workspace.templateFile(templateEngine, command, UTF_8, params);
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
