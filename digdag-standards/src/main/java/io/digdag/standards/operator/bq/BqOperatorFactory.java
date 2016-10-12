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
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigKey;
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
import java.util.function.Function;

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static io.digdag.standards.operator.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.PollingWaiter.pollingWaiter;
import static java.nio.charset.StandardCharsets.UTF_8;

public class BqOperatorFactory
        implements OperatorFactory
{
    private static final int MAX_JOB_ID_LENGTH = 1024;

    private static final String JOB_ID = "jobId";
    private static final String START = "start";
    private static final String RUNNING = "running";
    private static final String CHECK = "check";

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
            Optional<String> jobId = state.getOptional(JOB_ID, String.class);
            if (!jobId.isPresent()) {
                state.set(JOB_ID, uniqueJobId());
                throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
            }
            String canonicalJobId = projectId + ":" + jobId.get();

            // Start job
            pollingRetryExecutor(state, START)
                    .withErrorMessage("BigQuery job submission failed: %s", canonicalJobId)
                    .retryUnless(GoogleJsonResponseException.class, e -> e.getStatusCode() / 100 == 4)
                    .runOnce(() -> {
                        Job job = jobRequest(projectId, jobId.get());
                        logger.info("Submitting BigQuery job: {}", canonicalJobId);
                        try {
                            bigquery.jobs()
                                    .insert(projectId, job)
                                    .execute();
                        }
                        catch (GoogleJsonResponseException e) {
                            if (e.getStatusCode() == 409) {
                                // Already started
                                logger.debug("BigQuery job already started: {}", canonicalJobId, e);
                                return;
                            }
                            throw e;
                        }
                    });

            // Wait for job to complete
            Job completed = pollingWaiter(state, RUNNING)
                    .withWaitMessage("BigQuery job still running: %s", jobId)
                    .awaitOnce(Job.class, pollState -> {

                        // Check job status
                        Job job = pollingRetryExecutor(pollState, CHECK)
                                .retryUnless(GoogleJsonResponseException.class, e -> e.getStatusCode() / 100 == 4)
                                .withErrorMessage("BigQuery job status check failed: %s", canonicalJobId)
                                .run(() -> {
                                    logger.info("Checking BigQuery job status: {}", canonicalJobId);
                                    return bigquery.jobs()
                                            .get(projectId, jobId.get())
                                            .execute();
                                });

                        // Done yet?
                        JobStatus status = job.getStatus();
                        switch (status.getState()) {
                            case "DONE":
                                return Optional.of(job);
                            case "PENDING":
                            case "RUNNING":
                                return Optional.absent();
                            default:
                                throw new TaskExecutionException("Unknown job state: " + canonicalJobId + ": " + status.getState(), ConfigElement.empty());
                        }
                    });

            // Check job result
            JobStatus status = completed.getStatus();
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
                logger.info("BigQuery job successfully done: {}", canonicalJobId);
            }
            return result(completed);
        }

        private Job jobRequest(String projectId, String jobId)
        {
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

            return new Job()
                    .setJobReference(reference)
                    .setConfiguration(jobConfig);
        }

        private TaskResult result(Job job)
        {
            ConfigFactory cf = request.getConfig().getFactory();
            Config result = cf.create();
            Config bq = result.getNestedOrSetEmpty("bq");
            bq.set("last_jobid", job.getId());
            return TaskResult.defaultBuilder(request)
                    .storeParams(result)
                    .addResetStoreParams(ConfigKey.of("bq", "last_jobid"))
                    .build();
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
