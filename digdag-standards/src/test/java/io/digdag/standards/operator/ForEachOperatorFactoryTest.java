package io.digdag.standards.operator;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

import io.digdag.client.config.Config;
import io.digdag.core.DigdagEmbed;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.ForEachOperatorFactory.ForEachOperator;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static io.digdag.core.workflow.WorkflowTestingUtils.runWorkflow;

public class ForEachOperatorFactoryTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private Path tempPath;
    private ForEachOperatorFactory factory;

    @Before
    public void createInstance()
    {
        this.factory = newOperatorFactory(ForEachOperatorFactory.class);
        this.tempPath = folder.getRoot().toPath();
    }

    private void assertByResource(String configResource, String expectedResource)
        throws Exception
    {
        ForEachOperator op = factory.newOperator(newContext(
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
    public void testBasic()
        throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/basic.yml",
                "/io/digdag/standards/operator/for_each/basic_expected.yml");
    }

    @Test
    public void testDuplicateValues()
        throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/duplicate_values.yml",
                "/io/digdag/standards/operator/for_each/duplicate_values_expected.yml");
    }

    @Test
    public void parallelComplex()
        throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/parallel_complex.yml",
                "/io/digdag/standards/operator/for_each/parallel_complex_expected.yml");
    }

    @Test
    public void parallelLimitComplex()
            throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/parallel_limit_complex.yml",
                "/io/digdag/standards/operator/for_each/parallel_limit_complex_expected.yml");
    }

    @Test
    public void parseNestedMap()
        throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/parse_nested_map.yml",
                "/io/digdag/standards/operator/for_each/parse_nested_map_expected.yml");
    }

    @Test
    public void parseNestedArrays()
        throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/parse_nested_arrays.yml",
                "/io/digdag/standards/operator/for_each/parse_nested_arrays_expected.yml");
    }

    @Test
    public void escapeValues()
        throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/escape_values.yml",
                "/io/digdag/standards/operator/for_each/escape_values_expected.yml");
    }

    @Test
    public void escapeKeys()
        throws Exception
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/escape_keys.yml",
                "/io/digdag/standards/operator/for_each/escape_keys_expected.yml");
    }
}
