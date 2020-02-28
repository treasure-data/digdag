package io.digdag.standards.operator;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigUtils;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.spi.Operator;
import io.digdag.spi.TaskResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.*;
import static io.digdag.core.workflow.WorkflowTestingUtils.runWorkflow;
import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class HttpCallOperatorFactoryTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(0);

    private Path tempPath;
    private HttpCallOperatorFactory factory;

    @Before
    public void createInstance()
    {
        this.tempPath = folder.getRoot().toPath();
        this.factory = newOperatorFactory(HttpCallOperatorFactory.class);
    }

    private void assertResult(Config config, String expected)
        throws Exception
    {
        Operator op = factory.newOperator(newContext(
            tempPath, newTaskRequest().withConfig(config)));
        TaskResult result = op.run();
        Config subtasks = result.getSubtaskConfig();
        Config expectedConfig = new YamlConfigLoader().loadString(expected).toConfig(ConfigUtils.configFactory);

        assertThat(subtasks, is(expectedConfig));

        try (DigdagEmbed embed = setupEmbed()) {
            assertTrue(
                runWorkflow(embed, tempPath, "test", subtasks)
                    .getStateFlags()
                    .isSuccess()
            );
        }
    }

    @Test
    public void testBasic() throws Exception
    {
        Config config = newConfig();
        config.set("_command", "http://localhost:" + wireMockRule.port() + "/api/foobar");
        config.set("timeout", 3);
        config.set("retry", false);

        String expected = "+sample:\n  echo>: 'hello'";

        stubFor(get("/api/foobar")
            .willReturn(aResponse()
                .withHeader("Content-Type", "application/x-yaml")
                .withBody(expected)
                .withStatus(200)));

        assertResult(config, expected);
    }

}
