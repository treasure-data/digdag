package io.digdag.core.workflow;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.io.Resources;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.LocalSite;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigUtils;
import io.digdag.core.DigdagEmbed;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.is;

public class WorkflowExecutorTest
{
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
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private LocalSite localSite;
    private TransactionManager tm;

    @Before
    public void setUp()
        throws Exception
    {
        this.localSite = embed.getLocalSite();
        this.tm = embed.getTransactionManager();
    }

    @Test
    public void run()
        throws Exception
    {
        runWorkflow("basic", loadYamlResource("/io/digdag/core/workflow/basic.dig"));
    }

    @Test
    public void validateUnknownTopLevelKeys()
        throws Exception
    {
        exception.expect(ConfigException.class);
        runWorkflow("unknown_schedule", newConfig()
                .set("invalid_schedule", newConfig().set("daily>", "00:00:00"))
                .set("+step1", newConfig().set("sh>", "echo ok"))
                );
    }

    @Test
    public void retryOnGroupingTask()
        throws Exception
    {
        runWorkflow("retry_on_group", loadYamlResource("/io/digdag/core/workflow/retry_on_group.dig"));
        assertThat(new String(Files.readAllBytes(folder.getRoot().toPath().resolve("out")), UTF_8), is("try1try2try1try2try1try2try1try2"));
    }

    @Test
    public void retryIntervalOnGroupingTask()
            throws Exception
    {
        class TestSet
        {
            String retryStatement;
            int minWaitSecs;
            public TestSet(String retryStatement, int minWaitSecs)
            {
                this.retryStatement = retryStatement;
                this.minWaitSecs = minWaitSecs;
            }
        }

        String contentBase = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/retry_interval_on_group.dig"), UTF_8);

        List<TestSet> retries = new ArrayList<TestSet>(Arrays.asList(
                new TestSet("_retry:\n    limit: \"3\"\n    interval: \"2\"", 6), // variable ${..} is replaced as string
                new TestSet("_retry:\n    limit: 3\n    interval: 2", 6), //retry will happen 3 times. 3 x 2sec = 6sec
                new TestSet("_retry:\n    limit: 3\n    interval: 1\n    interval_type: exponential", 7) // 1 + 2 + 4 = 7sec
        ));

        for (TestSet ts : retries) {
            String content = contentBase.replaceFirst("_retry:\\s+\\d+", ts.retryStatement);
            Config config =  new YamlConfigLoader().loadString(content).toConfig(ConfigUtils.configFactory);
            long startTime = System.currentTimeMillis();
            runWorkflow("retry_interval_on_group", config);
            long endTime = System.currentTimeMillis();
            assertThat(endTime - startTime , greaterThanOrEqualTo(ts.minWaitSecs * 1000L));
            Path outPath = folder.getRoot().toPath().resolve("out");
            assertThat(new String(Files.readAllBytes(outPath), UTF_8), is("try1try2try1try2try1try2try1try2"));
            Files.deleteIfExists(outPath);
        }
    }


    @Test
    public void ifOperatorDelayedEvalDo()
            throws Exception
    {
        runWorkflow("if_operator", loadYamlResource("/io/digdag/core/workflow/if_operator_do.dig"));
        assertThat(new String(Files.readAllBytes(folder.getRoot().toPath().resolve("out")), UTF_8), is("OK_do"));

    }

    @Test
    public void ifOperatorDelayedEvalElseDo()
            throws Exception
    {
        runWorkflow("if_operator", loadYamlResource("/io/digdag/core/workflow/if_operator_else_do.dig"));
        assertThat(new String(Files.readAllBytes(folder.getRoot().toPath().resolve("out")), UTF_8), is("OK_else_do"));

    }


    private void runWorkflow(String workflowName, Config config)
            throws Exception
    {
        WorkflowTestingUtils.runWorkflow(embed, folder.getRoot().toPath(), workflowName, config);
    }
}
