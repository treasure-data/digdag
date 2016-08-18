package io.digdag.standards.operator.td;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJob;
import com.treasuredata.client.model.TDJobSummary;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TDOperatorTest
{
    // TODO: update these tests to use secrets

    private static final ImmutableMap<String, String> EMPTY_ENV = ImmutableMap.of();
    private static final SecretProvider EMPTY_SECRETS = key -> Optional.absent();

    @Rule public final ExpectedException exception = ExpectedException.none();

    @Mock TDClient client;
    @Mock TDOperator.JobStarter jobStarter;

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
                .set("database", "")
                .set("apikey", "foobar");

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(EMPTY_ENV, config, EMPTY_SECRETS);
    }

    @Test
    public void verifyWhitespaceDatabaseParameterIsRejected()
            throws Exception
    {
        Config config = newConfig()
                .set("database", " \t\n")
                .set("apikey", "foobar");

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(EMPTY_ENV, config, EMPTY_SECRETS);
    }

    @Test
    public void verifyEmptyApiKeyParameterIsRejected()
            throws Exception
    {
        Config config = newConfig()
                .set("database", "foobar")
                .set("apikey", "");

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(EMPTY_ENV, config, EMPTY_SECRETS);
    }

    @Test
    public void verifyWhitespaceApiKeyParameterIsRejected()
            throws Exception
    {
        Config config = newConfig()
                .set("database", "foobar")
                .set("apikey", " \n\t");

        exception.expect(ConfigException.class);
        TDOperator.fromConfig(EMPTY_ENV, config, EMPTY_SECRETS);
    }

    @Test
    public void testFromConfig()
            throws Exception
    {
        Config config = newConfig()
                .set("database", "foobar")
                .set("apikey", "quux");
        TDOperator.fromConfig(EMPTY_ENV, config, EMPTY_SECRETS);
    }

    @Test
    public void testRunJob()
            throws Exception
    {
        TDOperator operator = new TDOperator(client, "foobar");

        Config state0 = configFactory.create();

        String jobStateKey = "fooJob";

        // 1. Create domain key
        TDOperator.JobState jobState1;
        Config state1;
        {
            TaskExecutionException e = runJobIteration(operator, state0, jobStateKey, jobStarter);
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
            TaskExecutionException e = runJobIteration(operator, state1, jobStateKey, jobStarter);
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
            TaskExecutionException e = runJobIteration(operator, state2, jobStateKey, jobStarter);
            state3 = e.getStateParams(configFactory).get();
            jobState3 = state3.get(jobStateKey, TDOperator.JobState.class);
            assertThat(jobState3.pollIteration(), is(Optional.of(1)));
        }

        // 3. Check job status (SUCCESS)
        when(client.jobStatus(jobId)).thenReturn(summary(jobId, TDJob.Status.SUCCESS));
        TDJobOperator jobOperator = operator.runJob(state3, jobStateKey, jobStarter);
        assertThat(jobOperator.getJobId(), is(jobId));

        verifyNoMoreInteractions(jobStarter);
    }

    @Test
    public void testRunJobMigrateState()
            throws Exception
    {
        TDOperator operator = new TDOperator(client, "foobar");

        Config state0 = configFactory.create();

        String jobId = "4711";
        String domainKey = "badf00d";
        int pollIteration = 17;

        state0.set("jobId", jobId);
        state0.set("domainKey", domainKey);
        state0.set("pollIteration", pollIteration);

        when(client.jobStatus(jobId)).thenReturn(summary(jobId, TDJob.Status.RUNNING));
        TaskExecutionException e = runJobIteration(operator, state0, "foobar", jobStarter);

        Config state1 = e.getStateParams(configFactory).get();
        assertThat(state1.has("jobId"), is(false));
        assertThat(state1.has("domainKey"), is(false));
        assertThat(state1.has("pollIteration"), is(false));
        assertThat(state1.get("foobar", TDOperator.JobState.class), is(TDOperator.JobState.empty()
                .withJobId(jobId)
                .withDomainKey(domainKey)
                .withPollIteration(pollIteration + 1)));
    }

    private TDJobSummary summary(String jobId, TDJob.Status status) {return new TDJobSummary(status, 0, 0, jobId, "", "", "", "");}

    private static TaskExecutionException runJobIteration(TDOperator operator, Config state, String key, TDOperator.JobStarter jobStarter)
    {
        try {
            operator.runJob(state, key, jobStarter);
        }
        catch (TaskExecutionException e) {
            assertThat(e.getRetryInterval().isPresent(), is(true));
            return e;
        }
        return null;
    }
}
