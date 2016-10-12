package io.digdag.standards.operator;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.nio.file.Paths;

import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.ForEachOperatorFactory.ForEachOperator;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newOperatorFactory;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;

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
    {
        ForEachOperator op = factory.newOperator(tempPath, newTaskRequest().withConfig(
                    loadYamlResource(configResource)));
        TaskResult result = op.run(newContext());
        assertThat(result.getSubtaskConfig(), is(
                    loadYamlResource(expectedResource)));
    }

    @Test
    public void testBasic()
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/basic.yml",
                "/io/digdag/standards/operator/for_each/basic_expected.yml");
    }

    @Test
    public void parallelComplex()
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/parallel_complex.yml",
                "/io/digdag/standards/operator/for_each/parallel_complex_expected.yml");
    }

    @Test
    public void parseNestedMap()
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/parse_nested_map.yml",
                "/io/digdag/standards/operator/for_each/parse_nested_map_expected.yml");
    }

    @Test
    public void parseNestedArrays()
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/parse_nested_arrays.yml",
                "/io/digdag/standards/operator/for_each/parse_nested_arrays_expected.yml");
    }

    @Test
    public void escapeValues()
    {
        assertByResource(
                "/io/digdag/standards/operator/for_each/escape_values.yml",
                "/io/digdag/standards/operator/for_each/escape_values_expected.yml");
    }
}
