package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.agent.GrantedPrivilegedVariables;
import io.digdag.core.workflow.WorkflowTestingUtils;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.command.MockNonBlockingCommandExecutor;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.file.Path;

import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class NonBlockingPyOperatorTest
{
    private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

    public static DigdagEmbed embed;

    @BeforeClass
    public static void createDigdagEmbed()
    {
        embed = WorkflowTestingUtils.setupEmbed();
    }

    @AfterClass
    public static void destroyDigdagEmbed()
            throws Exception
    {
        embed.close();
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private ObjectMapper mapper;
    private ConfigFactory configFactory;
    private MockNonBlockingCommandExecutor executor;
    private PyOperatorFactory factory;
    private Path tempPath;

    @Before
    public void createInstance()
    {
        this.mapper = embed.getInjector().getInstance(ObjectMapper.class);
        this.configFactory = new ConfigFactory(mapper);
        this.executor = new MockNonBlockingCommandExecutor(mapper);
        this.factory = new PyOperatorFactory(executor, mapper);
        this.tempPath = folder.getRoot().toPath();
    }

    @Test
    public void testRunCommandSuccessfully()
            throws Exception
    {
        final String configResource = "/io/digdag/standards/operator/py/basic.yml";
        final PyOperatorFactory.PyOperator operator = (PyOperatorFactory.PyOperator) factory.newOperator(newContext(
                tempPath, newTaskRequest().withConfig(loadYamlResource(configResource))));
        final Config state0 = configFactory.create(); // empty state first

        // 1. run command
        Config state1;
        {
            final TaskExecutionException e = runCommandIteration(operator, state0);
            state1 = e.getStateParams(configFactory).get();
        }

        // 2. poll command status and still wait its complete
        Config state2;
        {
            final ObjectNode previousJson = state1.get("commandStatus", ObjectNode.class);
            final ObjectNode currentJson = (ObjectNode) previousJson.deepCopy()
                    .setAll(ImmutableMap.of(
                            "is_finished", jsonNodeFactory.booleanNode(false)
                    ));
            state1.set("commandStatus", currentJson);
            final TaskExecutionException e = runCommandIteration(operator, state1);
            state2 = e.getStateParams(configFactory).get();
        }

        // 3. poll command status and completed.
        {
            final ObjectNode previousJson = state2.get("commandStatus", ObjectNode.class);
            final ObjectNode currentJson = (ObjectNode) previousJson.deepCopy()
                    .setAll(ImmutableMap.of(
                            "is_finished", jsonNodeFactory.booleanNode(true)
                    ));
            state2.set("commandStatus", currentJson);
            assertNull(runCommandIteration(operator, state2));
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

    private static TaskExecutionException runCommandIteration(final PyOperatorFactory.PyOperator operator, final Config state)
            throws IOException, InterruptedException
    {
        try {
            PyOperatorFactory.runCodeForTesting(operator, state);
        }
        catch (TaskExecutionException e) {
            assertThat(e.getRetryInterval().isPresent(), is(true));
            return e;
        }
        return null;
    }
}
