package io.digdag.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import java.nio.file.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BaseOperatorTest
{
    private static final int INTERVAL = 4711;

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Mock TaskRequest request;

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

        BaseOperator op = new BaseOperator(newContext(temporaryFolder.getRoot().toPath(), request))
        {
            @Override
            public TaskResult runTask()
            {
                throw ex;
            }
        };

        try {
            op.run();
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

        BaseOperator op = new BaseOperator(newContext(temporaryFolder.getRoot().toPath(), request))
        {
            @Override
            public TaskResult runTask()
            {
                throw ex;
            }
        };

        try {
            op.run();
            fail();
        }
        catch (TaskExecutionException e) {
            assertThat(e, is(ex));
            assertThat(e.isError(), is(false));
            assertThat(e.getRetryInterval().get(), is(INTERVAL));
        }
    }

    @Test
    public void verifyNonPollingRuntimeExceptionRemovesCommandStatusOnRetry()
            throws Exception
    {
        config.set("_retry", 10);

        ObjectNode commandStatus = new ObjectMapper().createObjectNode().put("foo", "bar");
        state.set("commandStatus", commandStatus);

        RuntimeException ex = new RuntimeException("Command failed");

        BaseOperator op = new BaseOperator(newContext(temporaryFolder.getRoot().toPath(), request))
        {
            @Override
            public TaskResult runTask()
            {
                throw ex;
            }
        };

        try {
            op.run();
            fail();
        }
        catch (TaskExecutionException e) {
            assertThat(e.isError(), is(true));
            assertThat(e.getCause(), is(ex));
            assertThat(e.getStateParams(configFactory).get().has("commandStatus"), is(false));
        }
    }

    private OperatorContext newContext(final Path projectPath, final TaskRequest taskRequest)
    {
        return new OperatorContext()
        {
            @Override
            public Path getProjectPath()
            {
                return projectPath;
            }

            @Override
            public TaskRequest getTaskRequest()
            {
                return taskRequest;
            }

            @Override
            public SecretProvider getSecrets()
            {
                return null;
            }

            @Override
            public PrivilegedVariables getPrivilegedVariables()
            {
                return null;
            }
        };
    }
}
