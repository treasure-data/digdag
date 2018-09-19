package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.agent.GrantedPrivilegedVariables;
import io.digdag.core.workflow.MockCommandExecutor;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Path;

import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NonBlockingShOperatorTest
{
    private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Mock MockCommandExecutor executor;

    private ShOperatorFactory factory;
    private Path tempPath;

    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new GuavaModule())
            .registerModule(new JacksonTimeModule());
    private final ConfigFactory configFactory = new ConfigFactory(mapper);

    @Before
    public void createInstance()
    {
        this.factory = new ShOperatorFactory(executor);
        this.tempPath = folder.getRoot().toPath();
    }

    @Test
    public void testRunCommandSuccessfully()
            throws Exception
    {
        final String configResource = "/io/digdag/standards/operator/sh/basic.yml";
        final ShOperatorFactory.ShOperator operator = (ShOperatorFactory.ShOperator) factory.newOperator(newContext(
                tempPath, newTaskRequest().withConfig(loadYamlResource(configResource))));
        final Config state0 = configFactory.create(); // empty state first

        when(executor.run(any(CommandContext.class), any(CommandRequest.class)))
                .thenReturn(new NotFinishedCommandStatus()); // for 1
        when(executor.poll(any(CommandContext.class), any(ObjectNode.class)))
                .thenReturn(new NotFinishedCommandStatus()) // for 2
                .thenReturn(new FinishedCommandStatus(0)); // for 3

        // 1. run command
        Config state1;
        {
            final TaskExecutionException e = runCommandIteration(operator, state0);
            state1 = e.getStateParams(configFactory).get();
        }

        // 2. poll command status and still wait its complete
        Config state2;
        {
            final TaskExecutionException e = runCommandIteration(operator, state1);
            state2 = e.getStateParams(configFactory).get();
        }

        // 3. poll command status and completed.
        {
            final TaskExecutionException e = runCommandIteration(operator, state2);
            assertNull(e);
        }
    }

    @Test
    public void testCommandFailure()
            throws Exception
    {
        final String configResource = "/io/digdag/standards/operator/sh/basic.yml";
        final ShOperatorFactory.ShOperator operator = (ShOperatorFactory.ShOperator) factory.newOperator(newContext(
                tempPath, newTaskRequest().withConfig(loadYamlResource(configResource))));
        final Config state0 = configFactory.create(); // empty state first

        when(executor.run(any(CommandContext.class), any(CommandRequest.class)))
                .thenReturn(new NotFinishedCommandStatus()); // for 1
        when(executor.poll(any(CommandContext.class), any(ObjectNode.class)))
                .thenReturn(new NotFinishedCommandStatus()) // for 2
                .thenReturn(new FinishedCommandStatus(1)); // for 3

        // 1. run command
        Config state1;
        {
            final TaskExecutionException e = runCommandIteration(operator, state0);
            state1 = e.getStateParams(configFactory).get();
        }

        // 2. poll command status and still wait its complete
        Config state2;
        {
            final TaskExecutionException e = runCommandIteration(operator, state1);
            state2 = e.getStateParams(configFactory).get();
        }

        // 3. poll command status and completed.
        {
            try {
                runCommandIteration(operator, state2);
                fail();
            }
            catch (Exception e) {
                assertTrue(e instanceof RuntimeException);
            }
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
            public PrivilegedVariables getPrivilegedVariables()
            {
                return GrantedPrivilegedVariables.empty();

            }

            @Override
            public SecretProvider getSecrets()
            {
                return null;
            }
        };
    }

    private static TaskExecutionException runCommandIteration(final ShOperatorFactory.ShOperator operator, final Config state)
            throws IOException, InterruptedException
    {
        try {
            ShOperatorFactory.runCodeForTesting(operator, state);
        }
        catch (TaskExecutionException e) {
            assertThat(e.getRetryInterval().isPresent(), is(true));
            return e;
        }
        return null;
    }

    static class NotFinishedCommandStatus
            implements CommandStatus
    {
        @Override
        public boolean isFinished()
        {
            return false;
        }

        @Override
        public int getStatusCode()
        {
            return 0;
        }

        @Override
        public String getIoDirectory()
        {
            return null;
        }

        @Override
        public ObjectNode toJson()
        {
            return (ObjectNode) jsonNodeFactory.objectNode()
                    .set("mock", jsonNodeFactory.textNode("mock_value"));
        }
    }

    static class FinishedCommandStatus
            implements CommandStatus
    {
        private final int exitCode;

        public FinishedCommandStatus(int exitCode)
        {
            this.exitCode = exitCode;
        }

        @Override
        public boolean isFinished()
        {
            return true;
        }

        @Override
        public int getStatusCode()
        {
            return exitCode;
        }

        @Override
        public String getIoDirectory()
        {
            return null;
        }

        @Override
        public ObjectNode toJson()
        {
            return (ObjectNode) jsonNodeFactory.objectNode()
                    .set("mock", jsonNodeFactory.textNode("mock_value"));
        }
    }
}
