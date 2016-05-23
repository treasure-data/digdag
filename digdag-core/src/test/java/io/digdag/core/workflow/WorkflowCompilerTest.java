package io.digdag.core.workflow;

import com.google.common.base.Throwables;
import com.google.common.io.Resources;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.DigConfigLoader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static java.nio.charset.StandardCharsets.UTF_8;

public class WorkflowCompilerTest
{
    @Rule public ExpectedException exception = ExpectedException.none();

    private DigdagEmbed embed;

    private WorkflowCompiler compiler;

    @Before
    public void setUp()
            throws Exception
    {
        embed = setupEmbed();
        compiler = new WorkflowCompiler();
    }

    private Config loadYamlResource(String name)
    {
        try {
            String content = Resources.toString(getClass().getResource(name), UTF_8);
            return embed.getInjector().getInstance(DigConfigLoader.class)
                    .loadString(content)
                    .toConfig(embed.getInjector().getInstance(ConfigFactory.class));
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Test
    public void verifySingleOperatorPasses()
    {
        Config config = loadYamlResource("/digdag/workflow/cases/single_operator.dig");
        compiler.compile("single_operator", config);
    }

    @Test
    public void verifyMultipleOperatorsFail()
    {
        Config config = loadYamlResource("/digdag/workflow/cases/multiple_operators.dig");
        exception.expect(ConfigException.class);
        compiler.compile("multiple_operators", config);
    }

    @Test
    public void verifyUnusedKeysInGroupingTask()
    {
        Config config = loadYamlResource("/digdag/workflow/cases/unused_keys_in_group.dig");
        exception.expect(ConfigException.class);
        compiler.compile("unused_keys_in_group", config);
    }
}
