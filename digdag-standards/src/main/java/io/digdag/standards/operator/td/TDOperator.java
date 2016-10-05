package io.digdag.standards.operator.td;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.TDClientHttpConflictException;
import com.treasuredata.client.TDClientHttpException;
import com.treasuredata.client.TDClientHttpNotFoundException;
import com.treasuredata.client.TDClientHttpUnauthorizedException;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.util.RetryExecutor;
import io.digdag.util.RetryExecutor.RetryGiveupException;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.UUID;

import static com.treasuredata.client.model.TDJob.Status.SUCCESS;
import static io.digdag.util.RetryExecutor.retryExecutor;

public class TDOperator
        implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(TDOperator.class);

    // TODO: adjust these retry intervals and limits when all td operators have persistent retry mechanisms implemented

    private static final int INITIAL_RETRY_WAIT = 500;
    private static final int MAX_RETRY_WAIT = 2000;
    private static final int MAX_RETRY_LIMIT = 3;

    private static final Integer INITIAL_POLL_INTERVAL = 1;
    private static final int MAX_POLL_INTERVAL = 30;

    public static TDOperator fromConfig(Map<String, String> env, Config config, SecretProvider secrets)
    {
        String database = secrets.getSecretOptional("database").or(config.get("database", String.class)).trim();
        if (database.isEmpty()) {
            throw new ConfigException("Parameter 'database' is empty");
        }

        TDClient client = TDClientFactory.clientFromConfig(env, config, secrets);

        return new TDOperator(client, database);
    }

    static final RetryExecutor defaultRetryExecutor = retryExecutor()
            .withInitialRetryWait(INITIAL_RETRY_WAIT)
            .withMaxRetryWait(MAX_RETRY_WAIT)
            .withRetryLimit(MAX_RETRY_LIMIT)
            .retryIf((exception) -> !isDeterministicClientException(exception));

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

    private final TDClient client;
    private final String database;

    TDOperator(TDClient client, String database)
    {
        this.client = client;
        this.database = database;
    }

    public TDOperator withDatabase(String anotherDatabase)
    {
        return new TDOperator(client, anotherDatabase);
    }

    public String getDatabase()
    {
        return database;
    }

    public void ensureDatabaseCreated(String name)
            throws TDClientException
    {
        try {
            defaultRetryExecutor.run(() -> client.createDatabase(name));
        }
        catch (RetryGiveupException ex) {
            if (ex.getCause() instanceof TDClientHttpConflictException) {
                // ignore
                return;
            }
            throw Throwables.propagate(ex.getCause());
        }
    }

    public void ensureDatabaseDeleted(String name)
            throws TDClientException
    {
        try {
            defaultRetryExecutor.run(() -> client.deleteDatabase(name));
        }
        catch (RetryGiveupException ex) {
            if (ex.getCause() instanceof TDClientHttpNotFoundException) {
                // ignore
                return;
            }
            throw Throwables.propagate(ex.getCause());
        }
    }

    public void ensureTableCreated(String tableName)
            throws TDClientException
    {
        try {
            // TODO set include_v=false option
            defaultRetryExecutor.run(() -> client.createTable(database, tableName));
        }
        catch (RetryGiveupException ex) {
            if (ex.getCause() instanceof TDClientHttpConflictException) {
                // ignore
                return;
            }
            throw Throwables.propagate(ex.getCause());
        }
    }

    public void ensureTableDeleted(String tableName)
            throws TDClientException
    {
        try {
            // TODO set include_v=false option
            defaultRetryExecutor.run(() -> client.deleteTable(database, tableName));
        }
        catch (RetryGiveupException ex) {
            if (ex.getCause() instanceof TDClientHttpNotFoundException) {
                // ignore
                return;
            }
            throw Throwables.propagate(ex.getCause());
        }
    }

    public boolean tableExists(String table)
    {
        try {
            return defaultRetryExecutor.run(() -> client.existsTable(database, table));
        }
        catch (RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
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
        try {
            return defaultRetryExecutor.run(() -> submitNewJob(submitter));
        }
        catch (RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
    }

    public TDJobOperator newJobOperator(String jobId)
    {
        return new TDJobOperator(client, jobId);
    }

    /**
     * Run a TD job in a polling non-blocking fashion. Throws TaskExecutionException.ofNextPolling with the passed in state until the job is done.
     */
    public TDJobOperator runJob(Config state, String key, JobStarter starter)
    {
        ///////////////////////////////////////////////////////////////////////////////////////////
        // TODO: remove this migration code
        if (state.has("jobId")) {
            Config jobState = state.getNestedOrSetEmpty(key);
            if (!jobState.isEmpty()) {
                throw new AssertionError();
            }
            jobState.setOptional("jobId", state.getOptional("jobId", String.class));
            jobState.setOptional("domainKey", state.getOptional("domainKey", String.class));
            jobState.setOptional("pollIteration", state.getOptional("pollIteration", Integer.class));
            state.remove("jobId");
            state.remove("domainKey");
            state.remove("pollIteration");
        }
        ///////////////////////////////////////////////////////////////////////////////////////////

        JobState jobState = state.get(key, JobState.class, JobState.empty());

        // 0. Generate and store domain key before starting the job

        Optional<String> domainKey = jobState.domainKey();
        if (!domainKey.isPresent()) {
            state.set(key, jobState.withDomainKey(UUID.randomUUID().toString()));
            throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
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
                throw errorPollingException(state, key, jobState);
            }

            // Reset error polling
            jobState = jobState.withErrorPollIteration(Optional.absent());

            state.set(key, jobState.withJobId(newJobId));
            throw TaskExecutionException.ofNextPolling(INITIAL_POLL_INTERVAL, ConfigElement.copyOf(state));
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
            throw errorPollingException(state, key, jobState);
        }

        // Reset error polling
        jobState = jobState.withErrorPollIteration(Optional.absent());

        if (!status.getStatus().isFinished()) {
            throw pollingException(state, key, jobState);
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
                throw errorPollingException(state, key, jobState);
            }
            String message = jobInfo.getCmdOut() + "\n" + jobInfo.getStdErr();
            throw new TaskExecutionException(message, ConfigElement.empty());
        }

        return job;
    }

    private TaskExecutionException pollingException(Config state, String key, JobState jobState)
    {
        int iteration = jobState.pollIteration().or(0);
        int interval = (int) Math.min(INITIAL_POLL_INTERVAL * Math.pow(2, iteration), MAX_POLL_INTERVAL);
        state.set(key, jobState.withPollIteration(iteration + 1));
        throw TaskExecutionException.ofNextPolling(interval, ConfigElement.copyOf(state));
    }

    private TaskExecutionException errorPollingException(Config state, String key, JobState jobState)
    {
        int iteration = jobState.errorPollIteration().or(0);
        int interval = (int) Math.min(INITIAL_POLL_INTERVAL * Math.pow(2, iteration), MAX_POLL_INTERVAL);
        state.set(key, jobState.withErrorPollIteration(iteration + 1));
        throw TaskExecutionException.ofNextPolling(interval, ConfigElement.copyOf(state));
    }

    static boolean isDeterministicClientException(Exception ex)
    {
        return ex instanceof TDClientHttpNotFoundException ||
                ex instanceof TDClientHttpConflictException ||
                ex instanceof TDClientHttpUnauthorizedException;
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
}
