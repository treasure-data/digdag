package io.digdag.standards.operator.td;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDBulkLoadSessionStartResult;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Stream;

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TdLoadOperatorFactory
        implements OperatorFactory
{
    private static final String JOB_ID = "jobId";
    private static final String DOMAIN_KEY = "domainKey";
    private static final String POLL_ITERATION = "pollIteration";

    private static final Integer INITIAL_POLL_INTERVAL = 1;
    private static final int MAX_POLL_INTERVAL = 30000;

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
        private final Config params;
        private final Optional<Config> config;
        private final Optional<String> name;
        private final Optional<String> command;

        private final Config state;
        private final Optional<String> existingJobId;
        private final Optional<String> existingDomainKey;

        private final Optional<String> sessionName;
        private final Optional<ObjectNode> embulkConfig;

        public TdLoadOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);

            params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("td"));

            config = params.getOptionalNested("config");
            name = params.getOptional("name", String.class);
            command = params.getOptional("_command", String.class);

            if (config.isPresent() && name.isPresent()) {
                throw new ConfigException("The parameters config and name cannot both be set");
            }

            this.state = request.getLastStateParams().deepCopy();

            this.existingJobId = state.getOptional(JOB_ID, String.class);
            this.existingDomainKey = state.getOptional(DOMAIN_KEY, String.class);

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
        public TaskResult runTask()
        {
            // TODO: TDOperator requires database to be configured but the database param is not necessary when using a connector session

            // TODO: A lot of code is duplicated from TDOperatorFactory. Refactor shared logic into reusable components.

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
            Config storeParams = request.getConfig().getFactory().create()
                    .set("td", request.getConfig().getFactory().create()
                            .set("last_job_id", j.getJobId()));

            return TaskResult.defaultBuilder(request)
                    .storeParams(storeParams)
                    .build();
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

        private String startJob(TDOperator op)
        {
            assert Stream.of(embulkConfig, sessionName).filter(Optional::isPresent).count() == 1;

            if (embulkConfig.isPresent()) {
                return startBulkLoad(params, embulkConfig.get());
            } else if (sessionName.isPresent()) {
                return startBulkLoadSession(params, sessionName.get(), request);
            } else {
                throw new AssertionError();
            }
        }

        private String startBulkLoadSession(Config params, String name, TaskRequest request)
        {
            try (TDClient client = TDClientFactory.clientFromConfig(params)) {

                TDBulkLoadSessionStartResult r =
                        client.startBulkLoadSession(name, request.getSessionTime().getEpochSecond());

                logger.info("Started bulk load session job name={}, id={}", name, r.getJobId());

                return r.getJobId();
            }
        }

        private String startBulkLoad(Config params, ObjectNode embulkConfig)
        {
            TableParam table = params.get("table", TableParam.class);

            try (TDOperator op = TDOperator.fromConfig(params)) {
                TDJobRequest req = TDJobRequest
                    .newBulkLoad(table.getDatabase().or(op.getDatabase()), table.getTable(), embulkConfig);

                TDJobOperator j = op.submitNewJob(req);
                logger.info("Started bulk load job id={}", j.getJobId());

                return j.getJobId();
            }
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
