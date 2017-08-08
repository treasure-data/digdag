package io.digdag.standards.operator;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IfOperatorFactoryTest
{
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


    @Test
    public void testTrue() throws IOException {
        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(loadYamlResource("/io/digdag/standards/operator/if/true.yml"))));
        try {
            TaskResult result = op.run();
            assertFalse(result.getSubtaskConfig().isEmpty());
        } catch (TaskExecutionException e) {
            fail(e.toString());
        }
    }

    @Test
    public void testTrueWithElseDo() throws IOException {
        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(loadYamlResource("/io/digdag/standards/operator/if/true_else.yml"))));
        try {
            TaskResult result = op.run();
            assertTrue(result.getSubtaskConfig().isEmpty());
        } catch (TaskExecutionException e) {
            fail(e.toString());
        }
    }

    @Test
    public void testFalse() throws IOException {
        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(loadYamlResource("/io/digdag/standards/operator/if/false.yml"))));
        try {
            TaskResult result = op.run();
            assertFalse(result.getSubtaskConfig().isEmpty());
        } catch (TaskExecutionException e) {
            fail(e.toString());
        }
    }

    @Test
    public void testFalseWithDo() throws IOException {
        Operator op = factory.newOperator(newContext(
                tempPath,
                newTaskRequest().withConfig(loadYamlResource("/io/digdag/standards/operator/if/false_do.yml"))));
        try {
            TaskResult result = op.run();
            assertTrue(result.getSubtaskConfig().isEmpty());
        } catch (TaskExecutionException e) {
            fail(e.toString());
        }
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
