package io.digdag.core.workflow;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    public void retryInCall()
            throws Exception
    {
        String childContent = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/retry_in_call_child.dig"), UTF_8);
        Path childPath = folder.getRoot().toPath().resolve("retry_in_call_child.dig");
        System.out.println(childPath.toAbsolutePath().toString());
        Files.write(childPath, childContent.getBytes(UTF_8));

        String parentBase = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/retry_in_call_parent.dig"), UTF_8);

        //Full path not work in AppVeyor
        //String parentContent = parentBase.replaceFirst("child_path: dummy", "child_path: " + childPath.toString());
        String parentContent = parentBase.replaceFirst("child_path: dummy", "child_path: " +  "retry_in_call_child.dig");

        Config parent = new YamlConfigLoader().loadString(parentContent).toConfig(ConfigUtils.configFactory);
        runWorkflow("retry_in_call", parent);
        Path outPath = folder.getRoot().toPath().resolve("out");
        assertThat(new String(Files.readAllBytes(outPath), UTF_8), is("try1try2try1try2try1try2"));
    }


    @Test
    public void retryInLoop()
            throws Exception
    {
        String content = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/retry_in_loop.dig"), UTF_8);
        Config config = new YamlConfigLoader().loadString(content).toConfig(ConfigUtils.configFactory);
        runWorkflow("retry_in_loop", config);
        Path outPath = folder.getRoot().toPath().resolve("out");
        String out = new String(Files.readAllBytes(outPath), UTF_8);
        assertThat(new String(Files.readAllBytes(outPath), UTF_8), is("loop0:try1try2succeeded.loop1:try1try2succeeded.loop2:try1try2try1try2try1try2"));
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
        runWorkflow("if_operator_else_do", loadYamlResource("/io/digdag/core/workflow/if_operator_else_do.dig"));
        assertThat(new String(Files.readAllBytes(folder.getRoot().toPath().resolve("out")), UTF_8), is("OK_else_do"));

    }


    private void runWorkflow(String workflowName, Config config)
            throws Exception
    {
        WorkflowTestingUtils.runWorkflow(embed, folder.getRoot().toPath(), workflowName, config);
    }
}
