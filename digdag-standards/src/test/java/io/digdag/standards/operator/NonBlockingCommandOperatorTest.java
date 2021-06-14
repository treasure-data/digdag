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

import java.nio.file.Path;
import java.util.function.Supplier;

import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NonBlockingCommandOperatorTest
{
    private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

    private static DigdagEmbed embed;

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
    private Path tempPath;

    @Before
    public void createInstance()
    {
        this.mapper = embed.getInjector().getInstance(ObjectMapper.class);
        this.configFactory = new ConfigFactory(mapper);
        this.executor = new MockNonBlockingCommandExecutor(mapper);
        this.tempPath = folder.getRoot().toPath();
    }

    private TaskExecutionException runIteration(Supplier f)
    {
        try {
            f.get();
        }
        catch (TaskExecutionException e) {
            assertThat(e.getRetryInterval().isPresent(), is(true));
            return e;
        }
        return null;
    }

    @Test
    public void testRunWithOutputSuccessfully()
            throws Exception
    {
        final String configResource = "/io/digdag/standards/operator/py/basic.yml";
        final PyOperatorFactory operatorFactory = new PyOperatorFactory(executor, mapper);
        final PyOperatorFactory.PyOperator operator = (PyOperatorFactory.PyOperator) operatorFactory.newOperator(
                newContext(tempPath, newTaskRequest().withConfig(loadYamlResource(configResource))));

        final Config state0 = configFactory.create(); // empty state first

        // 1. run command
        final Config state1;
        {
            final TaskExecutionException e = runIteration(() -> PyOperatorFactory.runCodeForTesting(operator, state0));
            state1 = e.getStateParams(configFactory).get();
        }

        // 2. poll command status and still wait its complete
        final Config state2;
        {
            final ObjectNode previousJson = state1.get("commandStatus", ObjectNode.class);
            final ObjectNode currentJson = (ObjectNode) previousJson.deepCopy()
                    .setAll(ImmutableMap.of(
                            "is_finished", jsonNodeFactory.booleanNode(false)
                    ));
            state1.set("commandStatus", currentJson);

            final TaskExecutionException e = runIteration(() -> PyOperatorFactory.runCodeForTesting(operator, state1));
            state2 = e.getStateParams(configFactory).get();
        }

        // 3. poll command status and completed.
        {
            final ObjectNode previousJson = state2.get("commandStatus", ObjectNode.class);
            final ObjectNode currentJson = (ObjectNode) previousJson.deepCopy()
                    .setAll(ImmutableMap.of(
                            "is_finished", jsonNodeFactory.booleanNode(true),
                            "status_code", jsonNodeFactory.numberNode(0)
                    ));
            state2.set("commandStatus", currentJson);

            final TaskExecutionException e = runIteration(() -> PyOperatorFactory.runCodeForTesting(operator, state2));
            assertNull(e);
        }
    }

    @Test
    public void testRunCommandWithOutputFailure()
            throws Exception
    {
        final String configResource = "/io/digdag/standards/operator/py/basic.yml";
        final PyOperatorFactory operatorFactory = new PyOperatorFactory(executor, mapper);
        final PyOperatorFactory.PyOperator operator = (PyOperatorFactory.PyOperator) operatorFactory.newOperator(
                newContext(tempPath, newTaskRequest().withConfig(loadYamlResource(configResource))));

        final Config state0 = configFactory.create(); // empty state first

        // 1. run command
        final Config state1;
        {
            final TaskExecutionException e = runIteration(() -> PyOperatorFactory.runCodeForTesting(operator, state0));
            state1 = e.getStateParams(configFactory).get();
        }

        // 2. poll command status and still wait its complete
        final Config state2;
        {
            final ObjectNode previousJson = state1.get("commandStatus", ObjectNode.class);
            final ObjectNode currentJson = (ObjectNode) previousJson.deepCopy()
                    .setAll(ImmutableMap.of(
                            "is_finished", jsonNodeFactory.booleanNode(false)
                    ));
            state1.set("commandStatus", currentJson);

            final TaskExecutionException e = runIteration(() -> PyOperatorFactory.runCodeForTesting(operator, state1));
            state2 = e.getStateParams(configFactory).get();
        }

        // 3. poll command status and completed.
        {
            final ObjectNode previousJson = state2.get("commandStatus", ObjectNode.class);
            final ObjectNode currentJson = (ObjectNode) previousJson.deepCopy()
                    .setAll(ImmutableMap.of(
                            "is_finished", jsonNodeFactory.booleanNode(true),
                            "status_code", jsonNodeFactory.numberNode(1)
                    ));
            state2.set("commandStatus", currentJson);

            try {
                runIteration(() -> PyOperatorFactory.runCodeForTesting(operator, state2));
                fail();
            }
            catch (Exception e) {
                assertTrue(e instanceof RuntimeException);
            }
        }
    }

    @Test
    public void testRunCommandWithoutOutputSuccessfully()
            throws Exception
    {
        final String configResource = "/io/digdag/standards/operator/sh/basic.yml";
        final ShOperatorFactory operatorFactory = new ShOperatorFactory(executor);
        final ShOperatorFactory.ShOperator operator = (ShOperatorFactory.ShOperator) operatorFactory.newOperator(
                newContext(tempPath, newTaskRequest().withConfig(loadYamlResource(configResource))));

        final Config state0 = configFactory.create(); // empty state first

        // 1. run command
        final Config state1;
        {
            final TaskExecutionException e = runIteration(() -> ShOperatorFactory.runCodeForTesting(operator, state0));
            state1 = e.getStateParams(configFactory).get();
        }

        // 2. poll command status and still wait its complete
        final Config state2;
        {
            final ObjectNode previousJson = state1.get("commandStatus", ObjectNode.class);
            final ObjectNode currentJson = (ObjectNode) previousJson.deepCopy()
                    .setAll(ImmutableMap.of(
                            "is_finished", jsonNodeFactory.booleanNode(false)
                    ));
            state1.set("commandStatus", currentJson);

            final TaskExecutionException e = runIteration(() -> ShOperatorFactory.runCodeForTesting(operator, state1));
            state2 = e.getStateParams(configFactory).get();
        }

        // 3. poll command status and completed.
        {
            final ObjectNode previousJson = state2.get("commandStatus", ObjectNode.class);
            final ObjectNode currentJson = (ObjectNode) previousJson.deepCopy()
                    .setAll(ImmutableMap.of(
                            "is_finished", jsonNodeFactory.booleanNode(true),
                            "status_code", jsonNodeFactory.numberNode(0)
                    ));
            state2.set("commandStatus", currentJson);

            final TaskExecutionException e = runIteration(() -> ShOperatorFactory.runCodeForTesting(operator, state2));
            assertNull(e);
        }
    }

    @Test
    public void testRunCommandWithoutOutputFailure()
            throws Exception
    {
        final String configResource = "/io/digdag/standards/operator/sh/basic.yml";
        final ShOperatorFactory operatorFactory = new ShOperatorFactory(executor);
        final ShOperatorFactory.ShOperator operator = (ShOperatorFactory.ShOperator) operatorFactory.newOperator(
                newContext(tempPath, newTaskRequest().withConfig(loadYamlResource(configResource))));

        final Config state0 = configFactory.create(); // empty state first

        // 1. run command
        final Config state1;
        {
            final TaskExecutionException e = runIteration(() -> ShOperatorFactory.runCodeForTesting(operator, state0));
            state1 = e.getStateParams(configFactory).get();
        }

        // 2. poll command status and still wait its complete
        final Config state2;
        {
            final ObjectNode previousJson = state1.get("commandStatus", ObjectNode.class);
            final ObjectNode currentJson = (ObjectNode) previousJson.deepCopy()
                    .setAll(ImmutableMap.of(
                            "is_finished", jsonNodeFactory.booleanNode(false)
                    ));
            state1.set("commandStatus", currentJson);

            final TaskExecutionException e = runIteration(() -> ShOperatorFactory.runCodeForTesting(operator, state1));
            state2 = e.getStateParams(configFactory).get();
        }

        // 3. poll command status and completed.
        {
            final ObjectNode previousJson = state2.get("commandStatus", ObjectNode.class);
            final ObjectNode currentJson = (ObjectNode) previousJson.deepCopy()
                    .setAll(ImmutableMap.of(
                            "is_finished", jsonNodeFactory.booleanNode(true),
                            "status_code", jsonNodeFactory.numberNode(1)
                    ));
            state2.set("commandStatus", currentJson);


            try {
                runIteration(() -> ShOperatorFactory.runCodeForTesting(operator, state2));
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
}
