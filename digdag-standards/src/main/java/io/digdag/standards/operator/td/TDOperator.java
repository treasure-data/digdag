package io.digdag.standards.operator.td;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.TDClientHttpConflictException;
import com.treasuredata.client.TDClientHttpNotFoundException;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.commons.ThrowablesUtil;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.DurationParam;
import io.digdag.util.RetryExecutor.RetryGiveupException;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static com.treasuredata.client.model.TDJob.Status.SUCCESS;

public class TDOperator extends BaseTDOperator
        implements Closeable
{
    public interface SystemDefaultConfig
    {
        String getEndpoint();
    }

    private static final Logger logger = LoggerFactory.getLogger(TDOperator.class);

    // TODO: adjust these retry intervals and limits when all td operators have persistent retry mechanisms implemented

    public static TDOperator fromConfig(BaseTDClientFactory clientFactory, SystemDefaultConfig systemDefaultConfig, Map<String, String> env, Config config, SecretProvider secrets)
    {
        return fromConfig(clientFactory, systemDefaultConfig, env, config, secrets, true);
    }

    public static TDOperator fromConfig(BaseTDClientFactory clientFactory, SystemDefaultConfig systemDefaultConfig, Map<String, String> env, Config config, SecretProvider secrets, boolean databaseRequired)
    {
        String database = null;
        if (databaseRequired) {
            database = secrets.getSecretOptional("database").or(config.get("database", String.class)).trim();
            if (database.isEmpty()) {
                throw new ConfigException("Parameter 'database' is empty");
            }
        }

        TDClient client = clientFactory.createClient(systemDefaultConfig, env, config, secrets);

        return new TDOperator(client, database, secrets);
    }

    public static String escapeHiveIdent(String ident)
    {
        // TODO escape symbols in ident
        return "`" + ident + "`";
    }

    public static String escapePrestoIdent(String ident)
    {
        // TODO escape symbols in ident
        return "\"" + ident + "\"";
    }

    public static String escapeHiveTableName(TableParam table)
    {
        if (table.getDatabase().isPresent()) {
            return escapeHiveIdent(table.getDatabase().get()) + '.' + escapeHiveIdent(table.getTable());
        }
        else {
            return escapeHiveIdent(table.getTable());
        }
    }

    public static String escapePrestoTableName(TableParam table)
    {
        if (table.getDatabase().isPresent()) {
            return escapePrestoIdent(table.getDatabase().get()) + '.' + escapePrestoIdent(table.getTable());
        }
        else {
            return escapePrestoIdent(table.getTable());
        }
    }

    private final String database;

    TDOperator(TDClient client, String database, SecretProvider secrets)
    {
        this.client = client;
        this.database = database;
        this.secrets = secrets;
    }

    public TDOperator withDatabase(String anotherDatabase)
    {
        return new TDOperator(client, anotherDatabase, secrets);
    }

    public String getDatabase()
    {
        return database;
    }

    private void runWithRetry(Runnable op, Class ignoreException)
    {
        try {
            defaultRetryExecutor.run(() -> {
                try {
                    authenticatinRetryExecutor().run(() -> op.run());
                } catch (RetryGiveupException ex) {
                    throw ThrowablesUtil.propagate(ex.getCause());
                }
            });
        }
        catch (RetryGiveupException ex) {
            if (ignoreException.isInstance(ex.getCause())) {
                // ignore
                return;
            }
            throw ThrowablesUtil.propagate(ex.getCause());
        }
    }

    public void ensureDatabaseCreated(String name)
            throws TDClientException
    {
        runWithRetry(() -> client.createDatabase(name), TDClientHttpConflictException.class);
        // Check database existing and retry only once if it doesn't exit by session error(WM-763)
        if (!client.existsDatabase(name)){
            runWithRetry(() -> client.createDatabase(name), TDClientHttpConflictException.class);
        }
    }

    public void ensureDatabaseDeleted(String name)
            throws TDClientException
    {
        runWithRetry(() -> client.deleteDatabase(name), TDClientHttpNotFoundException.class);
    }

    public void ensureTableCreated(String tableName)
            throws TDClientException
    {
        // TODO set include_v=false option
        runWithRetry(() -> client.createTable(database, tableName), TDClientHttpConflictException.class);
        // Check table existing and retry only once if it doesn't exit by session error(WM-763)
        if (!client.existsTable(database, tableName)){
            runWithRetry(() -> client.createTable(database, tableName), TDClientHttpConflictException.class);
        }
    }

    public void ensureTableDeleted(String tableName)
            throws TDClientException
    {
        // TODO set include_v=false option
        runWithRetry(() -> client.deleteTable(database, tableName), TDClientHttpNotFoundException.class);
    }

    public void ensureExistentTableRenamed(String existentTable, String toName)
            throws TDClientException
    {
        runWithRetry(() -> client.renameTable(database, existentTable, toName, true), TDClientHttpNotFoundException.class);
    }

    public boolean tableExists(String table)
    {
        return callWithRetry(() -> client.existsTable(database, table));
    }

    public long lookupConnection(String name)
    {
        return callWithRetry(() -> client.lookupConnection(name));
    }

    private String submitNewJob(TDJobRequest request)
    {
        String jobId;
        try {
            jobId = client.submit(request);
        }
        catch (TDClientHttpConflictException e) {
            Optional<String> conflictsWith = e.getConflictsWith();
            if (conflictsWith.isPresent()) {
                jobId = conflictsWith.get();
            }
            else {
                throw e;
            }
        }

        return jobId;
    }

    private String submitNewJob(Submitter submitter)
    {
        try {
            return submitter.submit(client);
        }
        catch (TDClientHttpConflictException e) {
            Optional<String> conflictsWith = e.getConflictsWith();
            if (conflictsWith.isPresent()) {
                return conflictsWith.get();
            }
            else {
                throw e;
            }
        }
    }

    public String submitNewJobWithRetry(TDJobRequest req)
    {
        if (!req.getDomainKey().isPresent()) {
            throw new IllegalArgumentException("domain key must be set");
        }

        // TODO: refresh credentials if access token is expired
        return submitNewJobWithRetry(client -> submitNewJob(req));
    }

    public String submitNewJobWithRetry(Submitter submitter)
    {
        return callWithRetry(() -> submitNewJob(submitter));
    }

    public TDJobOperator newJobOperator(String jobId)
    {
        return new TDJobOperator(client, jobId, secrets);
    }

    /**
     * Run a TD job in a polling non-blocking fashion. Throws TaskExecutionException.ofNextPolling with the passed in state until the job is done.
     */
    public TDJobOperator runJob(TaskState state, String key, DurationInterval pollInterval, DurationInterval retryInterval, JobStarter starter)
    {
        ///////////////////////////////////////////////////////////////////////////////////////////
        // TODO: remove this migration code
        if (state.params().has("jobId")) {
            Config jobState = state.params().getNestedOrSetEmpty(key);
            if (!jobState.isEmpty()) {
                throw new AssertionError();
            }
            jobState.setOptional("jobId", state.params().getOptional("jobId", String.class));
            jobState.setOptional("domainKey", state.params().getOptional("domainKey", String.class));
            jobState.setOptional("pollIteration", state.params().getOptional("pollIteration", Integer.class));
            state.params().remove("jobId");
            state.params().remove("domainKey");
            state.params().remove("pollIteration");
        }
        ///////////////////////////////////////////////////////////////////////////////////////////

        JobState jobState = state.params().get(key, JobState.class, JobState.empty());

        // 0. Generate and store domain key before starting the job

        Optional<String> domainKey = jobState.domainKey();
        if (!domainKey.isPresent()) {
            state.params().set(key, jobState.withDomainKey(UUID.randomUUID().toString()));
            throw state.pollingTaskExecutionException(0);
        }

        // 1. Start the job

        Optional<String> jobId = jobState.jobId();
        if (!jobId.isPresent()) {
            assert domainKey.isPresent();

            String newJobId;
            try {
                newJobId = starter.startJob(this, domainKey.get());
            }
            catch (TDClientException e) {
                logger.warn("failed to start job: domainKey={}", domainKey.get(), e);
                if (isDeterministicClientException(e)) {
                    throw e;
                }
                throw errorPollingException(state, key, jobState, retryInterval);
            }

            // Reset error state
            jobState = jobState.withErrorPollIteration(Optional.absent());

            state.params().set(key, jobState.withJobId(newJobId));
            throw state.pollingTaskExecutionException((int) pollInterval.min().getSeconds());
        }

        // 2. Check if the job is done

        TDJobOperator job = newJobOperator(jobId.get());

        TDJobSummary status;
        try {
            status = job.checkStatus();
        }
        catch (TDClientException e) {
            logger.warn("failed to check job status: domainKey={}, jobId={}", domainKey.get(), jobId.get(), e);
            if (isDeterministicClientException(e)) {
                throw e;
            }
            throw errorPollingException(state, key, jobState, retryInterval);
        }
        catch (TaskExecutionException ex) {
            if (ex.getMessage().contains("HTTP request execution failed with code 401")) {
                updateApikey(secrets);
            }
            throw ThrowablesUtil.propagate(ex);
        }

        // Reset error state
        jobState = jobState.withErrorPollIteration(Optional.absent());

        if (!status.getStatus().isFinished()) {
            throw pollingException(state, key, jobState, pollInterval);
        }

        // 3. Fail the task if the job failed

        if (status.getStatus() != SUCCESS) {
            TDJob jobInfo;
            try {
                jobInfo = job.getJobInfo();
            }
            catch (TDClientException e) {
                logger.warn("failed to get job failure info: domainKey={}, jobId={}, status={}", domainKey.get(), jobId.get(), status.getStatus(), e);
                if (isDeterministicClientException(e)) {
                    throw e;
                }
                throw errorPollingException(state, key, jobState, retryInterval);
            }
            String message = jobInfo.getCmdOut() + "\n" + jobInfo.getStdErr();
            throw new TaskExecutionException(message);
        }

        return job;
    }

    private TaskExecutionException pollingException(TaskState state, String key, JobState jobState, DurationInterval pollInterval)
    {
        int iteration = jobState.pollIteration().or(0);
        int interval = exponentialBackoffInterval(pollInterval, iteration);
        state.params().set(key, jobState.withPollIteration(iteration + 1));
        throw state.pollingTaskExecutionException(interval);
    }

    private TaskExecutionException errorPollingException(TaskState state, String key, JobState jobState, DurationInterval retryInterval)
    {
        int iteration = jobState.errorPollIteration().or(0);
        int interval = exponentialBackoffInterval(retryInterval, iteration);
        state.params().set(key, jobState.withErrorPollIteration(iteration + 1));
        throw state.pollingTaskExecutionException(interval);
    }

    private static int exponentialBackoffInterval(DurationInterval pollInterval, int iteration)
    {
        return (int) Math.min(pollInterval.min().getSeconds() * Math.pow(2, iteration), pollInterval.max().getSeconds());
    }

    @Override
    public void close()
    {
        client.close();
    }

    public interface Submitter
    {
        String submit(TDClient client);
    }

    public interface JobStarter
    {
        String startJob(TDOperator op, String domainKey);
    }

    @Value.Immutable
    @Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
    @JsonSerialize(as = ImmutableJobState.class)
    @JsonDeserialize(as = ImmutableJobState.class)
    interface JobState
    {
        Optional<String> jobId();
        Optional<String> domainKey();
        Optional<Integer> pollIteration();
        Optional<Integer> errorPollIteration();

        JobState withJobId(String value);
        JobState withJobId(Optional<String> value);
        JobState withDomainKey(String value);
        JobState withDomainKey(Optional<String> value);
        JobState withPollIteration(int value);
        JobState withPollIteration(Optional<Integer> value);
        JobState withErrorPollIteration(int value);
        JobState withErrorPollIteration(Optional<Integer> value);

        static JobState empty()
        {
            return ImmutableJobState.builder().build();
        }
    }

    static final Duration DEFAULT_MIN_POLL_INTERVAL = Duration.ofSeconds(1);
    static final Duration DEFAULT_MAX_POLL_INTERVAL = Duration.ofSeconds(30);
    static final Duration DEFAULT_MIN_RETRY_INTERVAL = Duration.ofSeconds(1);
    static final Duration DEFAULT_MAX_RETRY_INTERVAL = Duration.ofSeconds(30);

    static DurationInterval pollInterval(Config systemConfig)
    {
        Duration min = systemConfig.getOptional("config.td.min_poll_interval", DurationParam.class)
                .transform(DurationParam::getDuration).or(DEFAULT_MIN_POLL_INTERVAL);
        Duration max = systemConfig.getOptional("config.td.max_poll_interval", DurationParam.class)
                .transform(DurationParam::getDuration).or(DEFAULT_MAX_POLL_INTERVAL);
        return DurationInterval.of(min, max);
    }

    static DurationInterval retryInterval(Config systemConfig)
    {
        Duration min = systemConfig.getOptional("config.td.min_retry_interval", DurationParam.class)
                .transform(DurationParam::getDuration).or(DEFAULT_MIN_RETRY_INTERVAL);
        Duration max = systemConfig.getOptional("config.td.max_retry_interval", DurationParam.class)
                .transform(DurationParam::getDuration).or(DEFAULT_MAX_RETRY_INTERVAL);
        return DurationInterval.of(min, max);
    }

    static SystemDefaultConfig systemDefaultConfig(Config systemConfig)
    {
        final String endpoint = systemConfig.get("config.td.default_endpoint", String.class, "api.treasuredata.com");

        return new SystemDefaultConfig()
        {
            @Override
            public String getEndpoint()
            {
                return endpoint;
            }
        };
    }
}
