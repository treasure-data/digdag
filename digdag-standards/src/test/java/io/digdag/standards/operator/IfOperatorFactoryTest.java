package io.digdag.standards.operator;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.executor.DigdagEmbed;
import io.digdag.spi.Operator;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.IfOperatorFactory.IfOperator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;

import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static io.digdag.core.workflow.WorkflowTestingUtils.runWorkflow;
import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class IfOperatorFactoryTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path tempPath;
    private IfOperatorFactory factory;
    private Config config;

    @Before
    public void createInstance()
    {
        this.tempPath = folder.getRoot().toPath();
        this.factory = newOperatorFactory(IfOperatorFactory.class);
    }


    private Config loadYamlResourceWithNull(String resourceName, ConfigFactory factory) {
        if(resourceName == null || resourceName.trim().equals("")){
            return factory.create();
        } else {
            return loadYamlResource(resourceName);
        }
    }

    private void assertByResource(String configResource, String expectedResource)
            throws Exception
    {
        IfOperator op = factory.newOperator(newContext(
                tempPath, newTaskRequest().withConfig(loadYamlResource(configResource))));
        TaskResult result = op.run();
        Config subtasks = result.getSubtaskConfig();

        assertThat(subtasks, is(loadYamlResourceWithNull(expectedResource, subtasks.getFactory())));

        try (DigdagEmbed embed = setupEmbed()) {
            Config dummyTask = subtasks.getFactory().create();
            dummyTask.set("+dummy", subtasks);
            assertTrue(
                    runWorkflow(embed, tempPath, "test", dummyTask)
                            .getStateFlags()
                            .isSuccess()
            );
        }
    }

    @Test
    public void testTrueDoOnly()
            throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/if/true_do_only.yml",
                "/io/digdag/standards/operator/if/true_do_only_expected.yml");
    }

    @Test
    public void testTrueWithElseDo()
            throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/if/true_do_else_do.yml",
                "/io/digdag/standards/operator/if/true_do_else_do_expected.yml");
    }

    @Test
    public void testFalseDoOnly()
            throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/if/false_do_only.yml",
                null);
    }

    @Test
    public void testFalseWithElseDo()
            throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/if/false_do_else_do.yml",
                "/io/digdag/standards/operator/if/false_do_else_do_expected.yml");
    }

    @Test
    public void testFail() throws IOException {
        try {
            Operator op = factory.newOperator(newContext(
                    tempPath,
                    newTaskRequest().withConfig(loadYamlResource("/io/digdag/standards/operator/if/fail.yml"))));
            op.run();
            fail();
        } catch (ConfigException ignore) {

        } catch (TaskExecutionException e) {
            fail();
        }
    }
}
