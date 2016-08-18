package io.digdag.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BaseOperatorTest
{
    private static final int INTERVAL = 4711;

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Mock TaskRequest request;
    @Mock TaskExecutionContext taskExecutionContext;

    private final ConfigFactory configFactory = new ConfigFactory(new ObjectMapper());
    private Config config;
    private Config state;

    @Before
    public void setUp()
            throws Exception
    {
        config = configFactory.create();
        state = configFactory.create();

        when(request.getConfig()).thenReturn(config);
        when(request.getLastStateParams()).thenReturn(state);
    }

    @Test
    public void verifyPollingTaskExecutionExceptionIsNotSwallowed()
            throws Exception
    {
        TaskExecutionException ex = TaskExecutionException.ofNextPolling(INTERVAL, ConfigElement.empty());

        BaseOperator op = new BaseOperator(temporaryFolder.getRoot().toPath(), request)
        {
            @Override
            public TaskResult runTask(TaskExecutionContext ctx)
            {
                throw ex;
            }
        };

        try {
            op.run(taskExecutionContext);
            fail();
        }
        catch (TaskExecutionException e) {
            assertThat(e, is(ex));
            assertThat(e.isError(), is(false));
            assertThat(e.getRetryInterval().get(), is(INTERVAL));
        }
    }

    @Test
    public void verifyPollingTaskExecutionExceptionIsNotSwallowedWhenRetriesAreEnabled()
            throws Exception
    {
        config.set("_retry", 10);

        TaskExecutionException ex = TaskExecutionException.ofNextPolling(INTERVAL, ConfigElement.empty());

        BaseOperator op = new BaseOperator(temporaryFolder.getRoot().toPath(), request)
        {
            @Override
            public TaskResult runTask(TaskExecutionContext ctx)
            {
                throw ex;
            }
        };

        try {
            op.run(taskExecutionContext);
            fail();
        }
        catch (TaskExecutionException e) {
            assertThat(e, is(ex));
            assertThat(e.isError(), is(false));
            assertThat(e.getRetryInterval().get(), is(INTERVAL));
        }
    }
}