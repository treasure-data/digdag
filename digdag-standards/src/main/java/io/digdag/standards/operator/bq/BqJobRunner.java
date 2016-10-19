package io.digdag.standards.operator.bq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.treasuredata.client.ProxyConfig;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.core.Environment;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.Proxies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static io.digdag.standards.operator.PollingRetryExecutor.pollingRetryExecutor;
import static io.digdag.standards.operator.PollingWaiter.pollingWaiter;
import static java.nio.charset.StandardCharsets.UTF_8;

class BqJobRunner
        implements AutoCloseable
{
    private static Logger logger = LoggerFactory.getLogger(BqJobRunner.class);

    private static final int MAX_JOB_ID_LENGTH = 1024;

    private static final String JOB_ID = "jobId";
    private static final String START = "start";
    private static final String RUNNING = "running";
    private static final String CHECK = "check";

    private final TaskRequest request;
    private final Config state;
    private final ObjectMapper objectMapper;
    private final Bigquery client;
    private final NetHttpTransport transport;
    private final String projectId;

    BqJobRunner(TaskRequest request, TaskExecutionContext ctx, ObjectMapper objectMapper, Map<String, String> environment)
    {
        this.request = Objects.requireNonNull(request, "request");
        Objects.requireNonNull(ctx, "ctx");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.state = request.getLastStateParams().deepCopy();
        this.transport = new NetHttpTransport.Builder()
                .setProxy(proxy(environment))
                .build();
        String credential = ctx.secrets().getSecret("gcp.credential");
        this.client = client(credential, transport);
        this.projectId = request.getLocalConfig().getOptional("project", String.class)
                .or(request.getConfig().getNestedOrGetEmpty("bq").getOptional("project", String.class))
                .or(projectId(credential, ctx));
    }

    @Override
    public void close()
    {
        try {
            transport.shutdown();
        }
        catch (IOException e) {
            logger.warn("Error shutting down BigQuery client", e);
        }
    }

    Bigquery client()
    {
        return client;
    }

    String projectId()
    {
        return projectId;
    }

    Job runJob(JobConfiguration config)
    {
        // Generate job id
        Optional<String> jobId = state.getOptional(JOB_ID, String.class);
        if (!jobId.isPresent()) {
            state.set(JOB_ID, uniqueJobId());
            throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
        }
        String canonicalJobId = projectId + ":" + jobId.get();

        JobReference reference = new JobReference()
                .setProjectId(projectId)
                .setJobId(jobId.get());

        // Start job
        pollingRetryExecutor(state, state, START)
                .withErrorMessage("BigQuery job submission failed: %s", canonicalJobId)
                .retryUnless(GoogleJsonResponseException.class, e -> e.getStatusCode() / 100 == 4)
                .runOnce(() -> {
                    logger.info("Submitting BigQuery job: {}", canonicalJobId);
                    Job job = new Job()
                            .setJobReference(reference)
                            .setConfiguration(config);

                    try {
                        client.jobs()
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
        Job completed = pollingWaiter(state, state, RUNNING)
                .withWaitMessage("BigQuery job still running: %s", jobId)
                .awaitOnce(Job.class, pollState -> {

                    // Check job status
                    Job job = pollingRetryExecutor(state, pollState, CHECK)
                            .retryUnless(GoogleJsonResponseException.class, e -> e.getStatusCode() / 100 == 4)
                            .withErrorMessage("BigQuery job status check failed: %s", canonicalJobId)
                            .run(() -> {
                                logger.info("Checking BigQuery job status: {}", canonicalJobId);
                                return client.jobs()
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
                logger.error(toPrettyString(error));
            }
            throw new TaskExecutionException("BigQuery job failed: " + canonicalJobId, errorConfig(status.getErrors()));
        }

        // Success
        logger.info("BigQuery job successfully done: {}", canonicalJobId);

        return completed;
    }

    private static ConfigElement errorConfig(List<ErrorProto> errors)
    {
        Map<String, String> map = ImmutableMap.of(
                "errors", errors.stream()
                        .map(error -> toPrettyString(error))
                        .collect(Collectors.joining(", ")));
        return ConfigElement.ofMap(map);
    }

    private static String toPrettyString(ErrorProto error)
    {
        try {
            return error.toPrettyString();
        }
        catch (IOException e) {
            return "<json error>";
        }
    }

    private String projectId(String credential, TaskExecutionContext ctx)
    {
        JsonNode credentialJson;
        try {
            credentialJson = objectMapper.readTree(credential);
        }
        catch (IOException e) {
            throw new TaskExecutionException("Unable to parse 'gcp.credential' secret", TaskExecutionException.buildExceptionErrorConfig(e));
        }

        JsonNode projectIdJson = credentialJson.get("project_id");

        return ctx.secrets().getSecretOptional("gcp.project").or(() -> {
            if (projectIdJson == null || !projectIdJson.isTextual()) {
                throw new TaskExecutionException("Missing 'gcp.project' secret", ConfigElement.empty());
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

    private static Bigquery client(String credentialJson, HttpTransport transport)
    {
        GoogleCredential credential;
        try {
            credential = GoogleCredential.fromStream(new ByteArrayInputStream(credentialJson.getBytes(UTF_8)));
            return client(credential, transport);
        }
        catch (IOException e) {
            throw new TaskExecutionException(e, buildExceptionErrorConfig(e));
        }
    }

    private static Bigquery client(GoogleCredential credential, HttpTransport transport)
            throws IOException
    {
        JsonFactory jsonFactory = new JacksonFactory();

        if (credential.createScopedRequired()) {
            credential = credential.createScoped(BigqueryScopes.all());
        }

        return new Bigquery.Builder(transport, jsonFactory, credential)
                .setApplicationName("Digdag")
                .build();
    }

    private static Proxy proxy(Map<String, String> environment)
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

    private static String truncate(String s, int n)
    {
        return s.substring(0, Math.min(s.length(), n));
    }

    void createDataset(Dataset dataset)
            throws IOException
    {
        try {
            client.datasets().insert(projectId, dataset)
                    .execute();
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_CONFLICT) {
                logger.debug("Dataset already exists: {}:{}", dataset.getDatasetReference());
            }
            else {
                throw e;
            }
        }
    }

    void emptyDataset(Dataset dataset)
            throws IOException
    {
        String projectId = Optional.fromNullable(dataset.getDatasetReference().getProjectId()).or(projectId());
        String datasetId = dataset.getDatasetReference().getDatasetId();
        deleteDataset(projectId, datasetId);
        createDataset(dataset);
    }

    void deleteTables(String projectId, String datasetId)
            throws IOException
    {
        Bigquery.Tables.List list = client.tables().list(projectId, datasetId);
        TableList tableList;
        do {
            tableList = list.execute();
            List<TableList.Tables> tables = tableList.getTables();
            if (tables != null) {
                for (TableList.Tables table : tables) {
                    deleteTable(projectId, datasetId, table.getTableReference().getTableId());
                }
            }
        }
        while (tableList.getNextPageToken() != null);
    }

    void deleteTable(String projectId, String datasetId, String tableId)
            throws IOException
    {
        try {
            client.tables().delete(projectId, datasetId, tableId).execute();
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                // Already deleted
                return;
            }
            throw e;
        }
    }

    boolean datasetExists(String projectId, String datasetId)
            throws IOException
    {
        try {
            client.datasets().get(projectId, datasetId).execute();
            return true;
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                return false;
            }
            throw e;
        }
    }

    void deleteDataset(String projectId, String datasetId)
            throws IOException
    {
        if (datasetExists(projectId, datasetId)) {
            deleteTables(projectId, datasetId);

            try {
                client.datasets().delete(projectId, datasetId).execute();
            }
            catch (GoogleJsonResponseException e) {
                if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
                    // Already deleted
                    return;
                }
                throw e;
            }
        }
    }

    void createTable(Table table)
            throws IOException
    {
        String datasetId = table.getTableReference().getDatasetId();
        try {
            client.tables().insert(projectId, datasetId, table)
                    .execute();
        }
        catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HttpStatusCodes.STATUS_CODE_CONFLICT) {
                logger.debug("Table already exists: {}:{}.{}", projectId, datasetId, table.getTableReference().getTableId());
            }
            else {
                throw e;
            }
        }
    }

    static class Factory
    {
        private final ObjectMapper objectMapper;
        private final Map<String, String> environment;

        @Inject
        public Factory(ObjectMapper objectMapper, @Environment Map<String, String> environment)
        {
            this.objectMapper = objectMapper;
            this.environment = environment;
        }

        BqJobRunner create(TaskRequest request, TaskExecutionContext ctx)
        {
            return new BqJobRunner(request, ctx, objectMapper, environment);
        }
    }
}
