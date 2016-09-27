package io.digdag.standards.operator.bq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.TableReference;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Environment;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.Proxies;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;

public class BqOperatorFactory
        implements OperatorFactory
{
    private static final int MAX_JOB_ID_LENGTH = 1024;

    private static final Integer INITIAL_POLL_INTERVAL = 1;
    private static final int MAX_POLL_INTERVAL = 30;

    private static final Integer INITIAL_RETRY_INTERVAL = 5;
    private static final int MAX_RETRY_INTERVAL = (int) MINUTES.toSeconds(5);

    private static final String JOB_STATE = "job";

    private static Logger logger = LoggerFactory.getLogger(BqOperatorFactory.class);

    private final TemplateEngine templateEngine;
    private final ObjectMapper objectMapper;

    private final Map<String, String> environment;

    @Inject
    public BqOperatorFactory(
            @Environment Map<String, String> environment,
            TemplateEngine templateEngine,
            ObjectMapper objectMapper)
    {
        this.environment = environment;
        this.templateEngine = templateEngine;
        this.objectMapper = objectMapper;
    }

    public String getType()
    {
        return "bq";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new BqOperator(projectPath, request);
    }

    private class BqOperator
            extends BaseOperator
    {
        private final Config params;
        private final String query;
        private final Config state;

        BqOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request);
            this.params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("bq"));
            this.query = workspace.templateCommand(templateEngine, params, "query", UTF_8);
            this.state = request.getLastStateParams();
        }

        @Override
        public List<String> secretSelectors()
        {
            return ImmutableList.of("gcp.*");
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
        {
            String projectId = projectId(ctx);
            Bigquery bigquery = bigqueryClient(ctx);

            // Generate job id
            BqJobState jobState = state.get(JOB_STATE, BqJobState.class, BqJobState.empty());
            if (!jobState.jobId().isPresent()) {
                state.set(JOB_STATE, jobState.withJobId(uniqueJobId()));
                throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
            }

            String jobId = jobState.jobId().get();
            String canonicalJobId = projectId + ":" + jobId;

            // Start job
            if (!jobState.started().or(false)) {
                JobConfigurationQuery queryConfig = new JobConfigurationQuery()
                        .setQuery(query);

                configure(params, "allow_large_results", Boolean.class, queryConfig::setAllowLargeResults);
                configure(params, "use_legacy_sql", Boolean.class, queryConfig::setUseLegacySql);
                configure(params, "use_query_cache", Boolean.class, queryConfig::setUseQueryCache);
                configure(params, "create_disposition", String.class, queryConfig::setCreateDisposition);
                configure(params, "write_disposition", String.class, queryConfig::setWriteDisposition);
                configure(params, "flatten_results", Boolean.class, queryConfig::setFlattenResults);
                configure(params, "maximum_billing_tier", Integer.class, queryConfig::setMaximumBillingTier);
                configure(params, "preserve_nulls", Boolean.class, queryConfig::setPreserveNulls);
                configure(params, "priority", String.class, queryConfig::setPriority);

                // TODO

                configure(params, "default_dataset", DatasetReference.class, queryConfig::setDefaultDataset);
                configure(params, "destination_table", TableReference.class, queryConfig::setDestinationTable);

                // TODO

//                tableDefinitions
//                userDefinedFunctionResources

                JobConfiguration jobConfig = new JobConfiguration()
                        .setQuery(queryConfig);

                JobReference reference = new JobReference()
                        .setProjectId(projectId)
                        .setJobId(jobId);

                Job job = new Job()
                        .setJobReference(reference)
                        .setConfiguration(jobConfig);

                logger.info("Submitting BigQuery job: {}", job);

                Job inserted;
                try {
                    inserted = bigquery.jobs()
                            .insert(projectId, job)
                            .execute();
                }
                catch (IOException e) {
                    if (e instanceof GoogleJsonResponseException) {
                        GoogleJsonResponseException re = (GoogleJsonResponseException) e;
                        if (re.getStatusCode() == 409) {
                            // Already started, store job started state and start polling job status
                            logger.debug("BigQuery job already started: {}", canonicalJobId, e);
                            state.set(JOB_STATE, jobState.withStarted(true));
                            throw TaskExecutionException.ofNextPolling(INITIAL_POLL_INTERVAL, ConfigElement.copyOf(state));
                        }
                        else if (re.getStatusCode() / 100 == 4) {
                            // Permanent error, do not retry
                            logger.error("BigQuery job submission failed: {}", canonicalJobId, e);
                            throw new TaskExecutionException(e, buildExceptionErrorConfig(e));
                        }
                    }
                    int iteration = jobState.retryIteration().or(0);
                    int interval = (int) Math.min(INITIAL_RETRY_INTERVAL * Math.pow(2, iteration), MAX_RETRY_INTERVAL);
                    logger.error("BigQuery job submission failed, retrying in {} seconds: {}", interval, canonicalJobId, e);
                    state.set(JOB_STATE, jobState.withRetryIteration(iteration + 1));
                    throw TaskExecutionException.ofNextPolling(interval, ConfigElement.copyOf(state));
                }

                // Clear retry state
                jobState = jobState.withRetryIteration(Optional.absent());

                // Verify that the job id was correctly set
                if (!canonicalJobId.equals(inserted.getId())) {
                    throw new AssertionError("BigQuery job ID mismatch: " + canonicalJobId + " != " + inserted.getId());
                }

                // Store job started state and start polling job status
                state.set(JOB_STATE, jobState.withStarted(true));
                throw TaskExecutionException.ofNextPolling(INITIAL_POLL_INTERVAL, ConfigElement.copyOf(state));
            }

            // Check job status
            logger.info("Checking BigQuery job status: {}", canonicalJobId);

            Job job;
            try {
                job = bigquery.jobs()
                        .get(projectId, jobId)
                        .execute();
            }
            catch (IOException e) {
                if (e instanceof GoogleJsonResponseException) {
                    GoogleJsonResponseException re = (GoogleJsonResponseException) e;
                    if (re.getStatusCode() / 100 == 4) {
                        logger.error("BigQuery job status check failed: {}", canonicalJobId, e);
                        throw new TaskExecutionException(e, buildExceptionErrorConfig(e));
                    }
                }

                int iteration = jobState.retryIteration().or(0);
                int interval = (int) Math.min(INITIAL_RETRY_INTERVAL * Math.pow(2, iteration), MAX_RETRY_INTERVAL);
                logger.error("BigQuery job submission failed, retrying in {} seconds: {}", interval, canonicalJobId, e);
                state.set(JOB_STATE, jobState.withRetryIteration(iteration + 1));
                throw TaskExecutionException.ofNextPolling(interval, ConfigElement.copyOf(state));
            }

            // Clear retry state
            jobState = jobState.withRetryIteration(Optional.absent());

            JobStatus status = job.getStatus();

            switch (status.getState()) {
                case "DONE":
                    if (status.getErrorResult() != null) {
                        // Failed
                        logger.error("BigQuery job failed: {}", canonicalJobId);
                        for (ErrorProto error : status.getErrors()) {
                            String errorString;
                            try {
                                errorString = error.toPrettyString();
                            }
                            catch (IOException e) {
                                errorString = "<json error>";
                            }
                            logger.error("{}", errorString);
                        }
                    }
                    else {
                        // Success
                        logger.error("BigQuery job successfully done: {}", canonicalJobId);
                    }
                    return result(job);
                case "PENDING":
                case "RUNNING":
                    int iteration = jobState.pollIteration().or(0);
                    int interval = (int) Math.min(INITIAL_POLL_INTERVAL * Math.pow(2, iteration), MAX_POLL_INTERVAL);
                    state.set(JOB_STATE, jobState.withPollIteration(iteration + 1));
                    logger.info("BigQuery job {}, checking again in {} seconds: {}", status.getState(), interval, canonicalJobId);
                    throw TaskExecutionException.ofNextPolling(interval, ConfigElement.copyOf(state));
                default:
                    throw new TaskExecutionException("Unknown job state: " + canonicalJobId + ": " + status.getState(), ConfigElement.empty());
            }
        }

        private TaskResult result(Job job)
        {
            ConfigFactory cf = request.getConfig().getFactory();
            Config result = cf.create();
            Config bq = result.getNestedOrSetEmpty("bq");
            bq.set("last_jobid", job.getId());
//            job.get
            return null;
        }

        private String projectId(TaskExecutionContext ctx)
        {
            String gcsCredential = ctx.secrets().getSecret("gcp.credential");
            JsonNode gcsCredentialJson;
            try {
                gcsCredentialJson = objectMapper.readTree(gcsCredential);
            }
            catch (IOException e) {
                throw new TaskExecutionException("Unable to parse 'gcp.credential' secret", TaskExecutionException.buildExceptionErrorConfig(e));
            }
            JsonNode projectIdJson = gcsCredentialJson.get("project_id");

            return ctx.secrets().getSecretOptional("gcp.project_id").or(() -> {
                if (projectIdJson == null || !projectIdJson.isTextual()) {
                    throw new TaskExecutionException("Missing 'gcp.project_id' secret", ConfigElement.empty());
                }
                return projectIdJson.asText();
            });
        }

        private String uniqueJobId()
        {
            String suffix = "_" + UUID.randomUUID().toString();
            String prefix = "digdag" +
                    "_s" + request.getSiteId() +
                    "_p_" + truncate(request.getProjectName().or(""), 256) +
                    "_w_" + truncate(request.getWorkflowName(), 256) +
                    "_t_" + request.getTaskId() +
                    "_a_" + request.getAttemptId();
            int maxPrefixLength = MAX_JOB_ID_LENGTH - suffix.length();
            return truncate(prefix, maxPrefixLength) + suffix;
        }

        private Bigquery bigqueryClient(TaskExecutionContext ctx)
        {
            GoogleCredential credential;
            try {
                String credentialJson = ctx.secrets().getSecret("gcp.credential");
                credential = GoogleCredential.fromStream(new ByteArrayInputStream(credentialJson.getBytes(UTF_8)));
                return bigqueryClient(credential);
            }
            catch (IOException e) {
                throw new TaskExecutionException(e, buildExceptionErrorConfig(e));
            }
        }

        private Bigquery bigqueryClient(GoogleCredential credential)
                throws IOException
        {
            HttpTransport transport = new NetHttpTransport.Builder()
                    .setProxy(proxy())
                    .build();

            JsonFactory jsonFactory = new JacksonFactory();

            if (credential.createScopedRequired()) {
                Collection<String> bigqueryScopes = BigqueryScopes.all();
                credential = credential.createScoped(bigqueryScopes);
            }

            return new Bigquery.Builder(transport, jsonFactory, credential)
                    .setApplicationName("Digdag")
                    .build();
        }

        private Proxy proxy()
        {
            Optional<ProxyConfig> proxyConfig = Proxies.proxyConfigFromEnv("https", environment);
            if (!proxyConfig.isPresent()) {
                return Proxy.NO_PROXY;
            }

            ProxyConfig cfg = proxyConfig.get();
            InetSocketAddress address = new InetSocketAddress(cfg.getHost(), cfg.getPort());
            Proxy proxy = new Proxy(Proxy.Type.HTTP, address);

            // TODO: support authenticated proxying
            Optional<String> user = cfg.getUser();
            Optional<String> password = cfg.getPassword();
            if (user.isPresent() || password.isPresent()) {
                logger.warn("Authenticated proxy is not supported");
            }

            return proxy;
        }
    }

    private static <T> void configure(Config params, String key, Class<T> cls, Function<T, ?> action)
    {
        Optional<T> value = params.getOptional(key, cls);
        if (value.isPresent()) {
            action.apply(value.get());
        }
    }

    private static String truncate(String s, int n)
    {
        return s.substring(0, Math.min(s.length(), n));
    }
}
