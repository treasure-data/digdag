package io.digdag.standards.operator.td;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.TDClientException;
import com.treasuredata.client.TDClientHttpException;
import com.treasuredata.client.TDClientHttpNotFoundException;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobSummary;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.standards.operator.td.TDOperator.SystemDefaultConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Duration;
import java.util.Date;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TDOperatorTest
{
    static SystemDefaultConfig DEFAULT_DEFAULT_SYSTEM_CONFIG = new SystemDefaultConfig()
    {
        @Override
        public String getEndpoint()
        {
            return "api.treasuredata.com";
        }
    };

    private static final ImmutableMap<String, String> EMPTY_ENV = ImmutableMap.of();
    private static final BaseTDClientFactory clientFactory = new TDClientFactory();

    @Rule public final ExpectedException exception = ExpectedException.none();

    @Mock TDClient client;
    @Mock TDOperator.JobStarter jobStarter;

    private final DurationInterval pollInterval = DurationInterval.of(Duration.ofSeconds(1), Duration.ofSeconds(30));
    private final DurationInterval retryInterval = DurationInterval.of(Duration.ofSeconds(1), Duration.ofSeconds(30));

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new GuavaModule())
            .registerModule(new JacksonTimeModule());

    private final ConfigFactory configFactory = new ConfigFactory(mapper);

    private Config newConfig()
    {
        return configFactory.create();
    }

    @Test
    public void verifyEmptyDatabaseParameterIsRejected()
            throws Exception
    {
        Config config = newConfig()
                .set("database", "");

        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "foobar").get(key));

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(clientFactory, DEFAULT_DEFAULT_SYSTEM_CONFIG, EMPTY_ENV, config, secrets);
    }

    @Test
    public void verifyWhitespaceDatabaseParameterIsRejected()
            throws Exception
    {
        Config config = newConfig()
                .set("database", " \t\n");

        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "foobar").get(key));

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(clientFactory, DEFAULT_DEFAULT_SYSTEM_CONFIG, EMPTY_ENV, config, secrets);
    }

    @Test
    public void verifyEmptyApiKeyParameterIsRejected()
            throws Exception
    {
        Config config = newConfig()
                .set("database", "foobar");

        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "").get(key));

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(clientFactory, DEFAULT_DEFAULT_SYSTEM_CONFIG, EMPTY_ENV, config, secrets);
    }

    @Test
    public void verifyWhitespaceApiKeyParameterIsRejected()
            throws Exception
    {
        Config config = newConfig()
                .set("database", "foobar");

        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", " \n\t").get(key));

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(clientFactory, DEFAULT_DEFAULT_SYSTEM_CONFIG, EMPTY_ENV, config, secrets);
    }

    @Test
    public void testFromConfig()
            throws Exception
    {
        Config config = newConfig()
                .set("database", "foobar");
        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "quux").get(key));
        TDOperator.fromConfig(clientFactory, DEFAULT_DEFAULT_SYSTEM_CONFIG, EMPTY_ENV, config, secrets);
    }

    @Test
    public void testRunJob()
            throws Exception
    {
        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "quux").get(key));

        TDOperator operator = new TDOperator(client, "foobar", secrets);

        Config state0 = configFactory.create();

        String jobStateKey = "fooJob";

        // 1. Create domain key
        TDOperator.JobState jobState1;
        Config state1;
        {
            TaskExecutionException e = runJobIteration(operator, state0, jobStateKey, pollInterval, retryInterval, jobStarter);
            verifyZeroInteractions(jobStarter);
            state1 = e.getStateParams(configFactory).get();
            assertThat(state1.has(jobStateKey), is(true));
            jobState1 = state1.get(jobStateKey, TDOperator.JobState.class);
            assertThat(jobState1.domainKey().isPresent(), is(true));
        }

        // 2. Start job with domain key created above
        String jobId = "badf00d4711";
        Config state2;
        TDOperator.JobState jobState2;
        {
            when(jobStarter.startJob(any(TDOperator.class), anyString())).thenReturn(jobId);
            TaskExecutionException e = runJobIteration(operator, state1, jobStateKey, pollInterval, retryInterval, jobStarter);
            state2 = e.getStateParams(configFactory).get();
            verify(jobStarter).startJob(operator, jobState1.domainKey().get());
            jobState2 = state2.get(jobStateKey, TDOperator.JobState.class);
            assertThat(jobState2.jobId(), is(Optional.of(jobId)));
        }

        // 3. Check job status (RUNNING)
        Config state3;
        TDOperator.JobState jobState3;
        {
            when(client.jobStatus(jobId)).thenReturn(summary(jobId, TDJob.Status.RUNNING));
            TaskExecutionException e = runJobIteration(operator, state2, jobStateKey, pollInterval, retryInterval, jobStarter);
            state3 = e.getStateParams(configFactory).get();
            jobState3 = state3.get(jobStateKey, TDOperator.JobState.class);
            assertThat(jobState3.pollIteration(), is(Optional.of(1)));
        }

        // 3. Check job status (SUCCESS)
        when(client.jobStatus(jobId)).thenReturn(summary(jobId, TDJob.Status.SUCCESS));
        TDJobOperator jobOperator = operator.runJob(TaskState.of(state3), jobStateKey, pollInterval, retryInterval, jobStarter);
        assertThat(jobOperator.getJobId(), is(jobId));

        verifyNoMoreInteractions(jobStarter);
    }

    @Test
    public void verifyRetries()
            throws Exception
    {
        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "quux").get(key));

        TDOperator operator = new TDOperator(client, "foobar", secrets);

        String jobStateKey = "fooJob";

        Config state = configFactory.create();

        // 1. Create domain key
        String domainKey;
        {
            TaskExecutionException e = runJobIteration(operator, state, jobStateKey, pollInterval, retryInterval, jobStarter);
            verifyZeroInteractions(jobStarter);
            state = e.getStateParams(configFactory).get();
            assertThat(state.has(jobStateKey), is(true));
            TDOperator.JobState jobState = state.get(jobStateKey, TDOperator.JobState.class);
            domainKey = jobState.domainKey().get();
        }

        int errorPollIteration = 0;

        // 2. Start job with domain key created above

        for (int i = 0; i < 7; i++) {

            // 2.a. Failure: Netsplit
            {
                errorPollIteration++;
                reset(jobStarter);
                when(jobStarter.startJob(any(TDOperator.class), anyString()))
                        .thenThrow(new TDClientException(TDClientException.ErrorType.EXECUTION_FAILURE, "Error"));
                TaskExecutionException e = runJobIteration(operator, state, jobStateKey, pollInterval, retryInterval, jobStarter);
                verify(jobStarter).startJob(operator, domainKey);
                state = e.getStateParams(configFactory).get();
                TDOperator.JobState jobState = state.get(jobStateKey, TDOperator.JobState.class);
                assertThat(jobState.domainKey(), is(Optional.of(domainKey)));
                assertThat(jobState.pollIteration(), is(Optional.absent()));
                assertThat(jobState.errorPollIteration(), is(Optional.of(errorPollIteration)));
            }

            // 2.b. Failure: 503
            {
                errorPollIteration++;
                reset(jobStarter);
                when(jobStarter.startJob(any(TDOperator.class), anyString()))
                        .thenThrow(new TDClientHttpException(TDClientException.ErrorType.SERVER_ERROR, "Service Unavailable", 503, null));
                TaskExecutionException e = runJobIteration(operator, state, jobStateKey, pollInterval, retryInterval, jobStarter);
                verify(jobStarter).startJob(operator, domainKey);
                state = e.getStateParams(configFactory).get();
                TDOperator.JobState jobState = state.get(jobStateKey, TDOperator.JobState.class);
                assertThat(jobState.domainKey(), is(Optional.of(domainKey)));
                assertThat(jobState.pollIteration(), is(Optional.absent()));
                assertThat(jobState.errorPollIteration(), is(Optional.of(errorPollIteration)));
            }
        }

        // 2.c Successfully start job
        String jobId = "badf00d4711";
        {
            errorPollIteration = 0;
            reset(jobStarter);
            when(jobStarter.startJob(any(TDOperator.class), anyString())).thenReturn(jobId);
            TaskExecutionException e = runJobIteration(operator, state, jobStateKey, pollInterval, retryInterval, jobStarter);
            verify(jobStarter).startJob(operator, domainKey);
            state = e.getStateParams(configFactory).get();
            TDOperator.JobState jobState = state.get(jobStateKey, TDOperator.JobState.class);
            assertThat(jobState.pollIteration(), is(Optional.absent()));
            assertThat(jobState.errorPollIteration(), is(Optional.absent()));
            assertThat(jobState.jobId(), is(Optional.of(jobId)));
        }

        // 3. Check job status

        Optional<Integer> pollIteration = Optional.absent();

        TDJob.Status[] statuses = {TDJob.Status.QUEUED, TDJob.Status.RUNNING};
        for (TDJob.Status status : statuses) {

            for (int i = 0; i < 2; i++) {

                // 3.a. Failure: Netsplit
                {
                    errorPollIteration++;
                    reset(client);
                    when(client.jobStatus(jobId))
                            .thenThrow(new TDClientException(TDClientException.ErrorType.EXECUTION_FAILURE, "Error"));
                    TaskExecutionException e = runJobIteration(operator, state, jobStateKey, pollInterval, retryInterval, jobStarter);
                    verify(client, times(4)).jobStatus(jobId);
                    state = e.getStateParams(configFactory).get();
                    TDOperator.JobState jobState = state.get(jobStateKey, TDOperator.JobState.class);
                    assertThat(jobState.domainKey(), is(Optional.of(domainKey)));
                    assertThat(jobState.pollIteration(), is(pollIteration));
                    assertThat(jobState.errorPollIteration(), is(Optional.of(errorPollIteration)));
                }

                // 3.b. Failure: 503
                {
                    errorPollIteration++;
                    reset(client);
                    when(client.jobStatus(jobId))
                            .thenThrow(new TDClientHttpException(TDClientException.ErrorType.SERVER_ERROR, "Service Unavailable", 503, null));
                    TaskExecutionException e = runJobIteration(operator, state, jobStateKey, pollInterval, retryInterval, jobStarter);
                    verify(client, times(4)).jobStatus(jobId);
                    state = e.getStateParams(configFactory).get();
                    TDOperator.JobState jobState = state.get(jobStateKey, TDOperator.JobState.class);
                    assertThat(jobState.domainKey(), is(Optional.of(domainKey)));
                    assertThat(jobState.pollIteration(), is(pollIteration));
                    assertThat(jobState.errorPollIteration(), is(Optional.of(errorPollIteration)));
                }
            }

            // 3.c. Successfully check status
            {
                pollIteration = pollIteration.transform(it -> it + 1).or(Optional.of(1));
                errorPollIteration = 0;
                reset(client);
                when(client.jobStatus(jobId)).thenReturn(summary(jobId, status));
                TaskExecutionException e = runJobIteration(operator, state, jobStateKey, pollInterval, retryInterval, jobStarter);
                verify(client).jobStatus(jobId);
                state = e.getStateParams(configFactory).get();
                TDOperator.JobState jobState = state.get(jobStateKey, TDOperator.JobState.class);
                assertThat(jobState.pollIteration(), is(pollIteration));
            }
        }


        // 3.d Job SUCCESS
        when(client.jobStatus(jobId)).thenReturn(summary(jobId, TDJob.Status.SUCCESS));
        TDJobOperator jobOperator = operator.runJob(TaskState.of(state), jobStateKey, pollInterval, retryInterval, jobStarter);
        assertThat(jobOperator.getJobId(), is(jobId));

        verifyNoMoreInteractions(jobStarter);
    }

    @Test
    public void checkAuthenticationErrorException()
            throws Exception
    {
        TDClientHttpException ex = new TDClientHttpException(TDClientException.ErrorType.AUTHENTICATION_FAILURE, "unauthorized", 401, new Date());
        boolean isAuthenticationError = TDOperator.isAuthenticationErrorException(ex);
        assertTrue(isAuthenticationError);

        ex = new TDClientHttpException(TDClientException.ErrorType.TARGET_NOT_FOUND, "not found", 404, new Date());
        isAuthenticationError = TDOperator.isAuthenticationErrorException(ex);
        assertFalse(isAuthenticationError);
    }

    @Test
    public void verifyNoRetryOn404()
            throws Exception
    {
        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "quux").get(key));

        TDOperator operator = new TDOperator(client, "foobar", secrets);

        String jobStateKey = "fooJob";

        Config state = configFactory.create();

        // Create domain key
        runJobIteration(operator, state, jobStateKey, pollInterval, retryInterval, jobStarter);

        // Start job: Fail with 404
        when(jobStarter.startJob(any(TDOperator.class), anyString()))
                .thenThrow(new TDClientHttpNotFoundException("Database Not Found"));
        exception.expect(TDClientHttpNotFoundException.class);
        operator.runJob(TaskState.of(state), jobStateKey, pollInterval, retryInterval, jobStarter);
    }

    @Test
    public void verifyNoRetryInvalidTableName()
            throws Exception
    {
        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "quux").get(key));

        TDOperator operator = new TDOperator(client, "foobar", secrets);

        String jobStateKey = "fooJob";

        Config state = configFactory.create();

        // Create domain key
        runJobIteration(operator, state, jobStateKey, pollInterval, retryInterval, jobStarter);

        // Start job: Fail with TDClientException with TDClientException.ErrorType.INVALID_INPUT
        when(jobStarter.startJob(any(TDOperator.class), anyString()))
                .thenThrow(new TDClientException(TDClientException.ErrorType.INVALID_INPUT, "Table name must follow this pattern ^([a-z0-9_]+)$: InsertIntoHere"));
        exception.expect(TDClientException.class);
        operator.runJob(TaskState.of(state), jobStateKey, pollInterval, retryInterval, jobStarter);
    }

    @Test
    public void testRunJobMigrateState()
            throws Exception
    {
        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "quux").get(key));

        TDOperator operator = new TDOperator(client, "foobar", secrets);

        Config state0 = configFactory.create();

        String jobId = "4711";
        String domainKey = "badf00d";
        int pollIteration = 17;

        state0.set("jobId", jobId);
        state0.set("domainKey", domainKey);
        state0.set("pollIteration", pollIteration);

        when(client.jobStatus(jobId)).thenReturn(summary(jobId, TDJob.Status.RUNNING));
        TaskExecutionException e = runJobIteration(operator, state0, "foobar", pollInterval, retryInterval, jobStarter);

        Config state1 = e.getStateParams(configFactory).get();
        assertThat(state1.has("jobId"), is(false));
        assertThat(state1.has("domainKey"), is(false));
        assertThat(state1.has("pollIteration"), is(false));
        assertThat(state1.get("foobar", TDOperator.JobState.class), is(TDOperator.JobState.empty()
                .withJobId(jobId)
                .withDomainKey(domainKey)
                .withPollIteration(pollIteration + 1)));
    }

    @Test
    public void testSystemDefaultConfig()
    {
        SystemDefaultConfig defaultDefaults = TDOperator.systemDefaultConfig(configFactory.create());
        assertThat(defaultDefaults.getEndpoint(), is("api.treasuredata.com"));

        Config overrideConfig = configFactory.create()
                .set("config.td.default_endpoint", "api.treasuredata.co.jp");
        SystemDefaultConfig overrides = TDOperator.systemDefaultConfig(overrideConfig);
        assertThat(overrides.getEndpoint(), is("api.treasuredata.co.jp"));
    }

    @Test
    public void testDatabaseCreateSuccess()
    {
        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "quux").get(key));
        doNothing().when(client).createDatabase(anyString());
        doReturn(true).when(client).existsDatabase(anyString());
        TDOperator operator = new TDOperator(client, "foobar", secrets);

        operator.ensureDatabaseCreated("test");

        verify(client, atMost(1)).createDatabase("test");
    }

    @Test
    public void testDatabaseCreateFail()
    {
        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "quux").get(key));
        doNothing().when(client).createDatabase(anyString());
        doReturn(false).when(client).existsDatabase(anyString());
        TDOperator operator = new TDOperator(client, "foobar", secrets);

        operator.ensureDatabaseCreated("test");

        verify(client, atLeast(2)).createDatabase("test");
    }

    @Test
    public void testTableCreateSuccess()
    {
        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "quux").get(key));
        doNothing().when(client).createTable(anyString(), anyString());
        doReturn(true).when(client).existsTable(anyString(), anyString());
        TDOperator operator = new TDOperator(client, "foobar", secrets);

        operator.ensureTableCreated("test");

        verify(client,atMost(1)).createTable(anyString(), anyString());
    }

    @Test
    public void testTableCreateFail()
    {
        SecretProvider secrets = key -> Optional.fromNullable(
                ImmutableMap.of("apikey", "quux").get(key));
        doNothing().when(client).createTable(anyString(), anyString());
        doReturn(false).when(client).existsTable(anyString(), anyString());
        TDOperator operator = new TDOperator(client, "foobar", secrets);

        operator.ensureTableCreated("test");

        verify(client,atLeast(2)).createTable(anyString(), anyString());
    }

    private TDJobSummary summary(String jobId, TDJob.Status status) {
        return new TDJobSummary(status, 0, 0, jobId, "", "", "", "");
    }

    private static TaskExecutionException runJobIteration(TDOperator operator, Config state, String key, DurationInterval pollInterval, DurationInterval retryInterval, TDOperator.JobStarter jobStarter)
    {
        try {
            operator.runJob(TaskState.of(state), key, pollInterval, retryInterval, jobStarter);
        }
        catch (TaskExecutionException e) {
            assertThat(e.getRetryInterval().isPresent(), is(true));
            return e;
        }
        return null;
    }
}
