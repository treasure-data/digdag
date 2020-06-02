package io.digdag.standards.operator;

import io.digdag.client.config.Config;
import io.digdag.executor.DigdagEmbed;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.ForRangeOperatorFactory.ForRangeOperator;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static io.digdag.core.workflow.WorkflowTestingUtils.runWorkflow;
import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ForRangeOperatorFactoryTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path tempPath;
    private ForRangeOperatorFactory factory;

    @Before
    public void createInstance()
    {
        this.factory = newOperatorFactory(ForRangeOperatorFactory.class);
        this.tempPath = folder.getRoot().toPath();
    }

    private void assertByResource(String configResource, String expectedResource)
        throws Exception
    {
        ForRangeOperator op = factory.newOperator(newContext(
                    tempPath, newTaskRequest().withConfig(loadYamlResource(configResource))));
        TaskResult result = op.run();
        Config subtasks = result.getSubtaskConfig();
        assertThat(subtasks, is(loadYamlResource(expectedResource)));

        try (DigdagEmbed embed = setupEmbed()) {
            assertTrue(
                    runWorkflow(embed, tempPath, "test", subtasks)
                            .getStateFlags()
                            .isSuccess()
            );
        }
    }

    @Test
    public void testSlices()
        throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/for_range/slices.yml",
                "/io/digdag/standards/operator/for_range/slices_expected.yml");
    }

    @Test
    public void testStep()
        throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/for_range/step.yml",
                "/io/digdag/standards/operator/for_range/step_expected.yml");
    }

    @Test
    public void testParallel()
        throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/for_range/parallel.yml",
                "/io/digdag/standards/operator/for_range/parallel_expected.yml");
    }

    @Test
    public void testParse()
        throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/for_range/parse.yml",
                "/io/digdag/standards/operator/for_range/parse_expected.yml");
    }
}
