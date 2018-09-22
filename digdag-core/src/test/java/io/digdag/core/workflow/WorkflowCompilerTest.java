package io.digdag.core.workflow;

import com.google.common.base.Throwables;
import com.google.common.io.Resources;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.YamlConfigLoader;
import org.junit.Before;
import org.junit.After;
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

    @After
    public void destroy()
        throws Exception
    {
        embed.close();
    }

    private Config loadYamlResource(String name)
    {
        try {
            String content = Resources.toString(getClass().getResource(name), UTF_8);
            return embed.getInjector().getInstance(YamlConfigLoader.class)
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
        Config config = loadYamlResource("/io/digdag/core/workflow/single_operator.dig");
        compiler.compile("single_operator", config);
    }

    @Test
    public void verifyMultipleOperatorsFail()
    {
        Config config = loadYamlResource("/io/digdag/core/workflow/multiple_operators.dig");
        exception.expect(ConfigException.class);
        compiler.compile("multiple_operators", config);
    }

    @Test
    public void verifyUnusedKeysInGroupingTask()
    {
        Config config = loadYamlResource("/io/digdag/core/workflow/unused_keys_in_group.dig");
        exception.expect(ConfigException.class);
        compiler.compile("unused_keys_in_group", config);
    }

    @Test
    public void verifyErrorTaskIsValidated()
    {
        Config config = loadYamlResource("/io/digdag/core/workflow/invalid_error_task.dig");
        exception.expect(ConfigException.class);
        compiler.compile("invalid_error_task", config);
    }
}
