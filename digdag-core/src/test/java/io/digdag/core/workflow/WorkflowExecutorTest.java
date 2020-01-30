package io.digdag.core.workflow;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.time.Duration;
import java.time.Instant;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.io.Resources;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.LocalSite;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigUtils;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredTask;
import io.digdag.metrics.StdDigdagMetrics;
import io.digdag.spi.metrics.DigdagMetrics;
import io.digdag.util.RetryControl;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.digdag.client.config.ConfigUtils.configFactory;
import static java.nio.charset.StandardCharsets.UTF_8;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkflowExecutorTest
{
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutorTest.class);

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
    public void retryExponential()
        throws Exception
    {
        Instant start = Instant.now();
        runWorkflow("retry_exponential", loadYamlResource("/io/digdag/core/workflow/retry_exponential.dig"));
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        // Duration should be longer than 11 sec and shorter than 63 sec:
        // with max_interval: 2: [1, 2, 2, 2, 2, 2].sum => 11
        // without max_interval: [1, 2, 4, 8, 16, 32].sum => 63
        assertThat(duration.getSeconds(), greaterThanOrEqualTo(11L));
        assertThat(duration.getSeconds(), lessThanOrEqualTo(63L));
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
    public void retryAllCall()
            throws Exception
    {
        String subFailContent = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_sub_fail.dig"), UTF_8);
        Path subFailPath = folder.getRoot().toPath().resolve("call_sub_fail.dig");
        System.out.println(subFailPath.toAbsolutePath().toString());
        Files.write(subFailPath, subFailContent.getBytes(UTF_8));

        String subSuccessContent = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_sub_success.dig"), UTF_8);
        Path subSuccessPath = folder.getRoot().toPath().resolve("call_sub_success.dig");
        System.out.println(subSuccessPath.toAbsolutePath().toString());
        Files.write(subSuccessPath, subSuccessContent.getBytes(UTF_8));

        String parentBase = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_main_retry.dig"), UTF_8);

        //Full path not work in AppVeyor
        //String parentContent = parentBase.replaceFirst("child_path: dummy", "child_path: " + childPath.toString());
        String parentContent = parentBase.replaceFirst("call_sub_success_path: dummy", "call_sub_success_path: " +  "call_sub_success.dig")
                                         .replaceFirst("call_sub_fail_path: dummy", "call_sub_fail_path: " +  "call_sub_fail.dig");

        Config parent = new YamlConfigLoader().loadString(parentContent).toConfig(ConfigUtils.configFactory);
        runWorkflow("call_main_retry", parent);
        Path outPath = folder.getRoot().toPath().resolve("out");
        assertThat(new String(Files.readAllBytes(outPath), UTF_8), is("successfailedsuccessfailedsuccessfailed"));
    }

    @Test
    public void retryOnlyFailedSub()
            throws Exception
    {
        String subFailContent = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_sub_fail_retry.dig"), UTF_8);
        Path subFailPath = folder.getRoot().toPath().resolve("call_sub_fail_retry.dig");
        System.out.println(subFailPath.toAbsolutePath().toString());
        Files.write(subFailPath, subFailContent.getBytes(UTF_8));

        String subSuccessContent = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_sub_success.dig"), UTF_8);
        Path subSuccessPath = folder.getRoot().toPath().resolve("call_sub_success.dig");
        System.out.println(subSuccessPath.toAbsolutePath().toString());
        Files.write(subSuccessPath, subSuccessContent.getBytes(UTF_8));

        String parentBase = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_main.dig"), UTF_8);

        //Full path not work in AppVeyor
        //String parentContent = parentBase.replaceFirst("child_path: dummy", "child_path: " + childPath.toString());
        String parentContent = parentBase.replaceFirst("call_sub_success_path: dummy", "call_sub_success_path: " +  "call_sub_success.dig")
                                         .replaceFirst("call_sub_fail_path: dummy", "call_sub_fail_path: " +  "call_sub_fail_retry.dig");

        Config parent = new YamlConfigLoader().loadString(parentContent).toConfig(ConfigUtils.configFactory);
        runWorkflow("call_main", parent);
        Path outPath = folder.getRoot().toPath().resolve("out");
        assertThat(new String(Files.readAllBytes(outPath), UTF_8), is("successfailedfailedfailed"));
    }

    @Test
    public void retryNestCall()
            throws Exception
    {
        String subFailContent = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_sub_fail.dig"), UTF_8);
        Path subFailPath = folder.getRoot().toPath().resolve("call_sub_fail.dig");
        System.out.println(subFailPath.toAbsolutePath().toString());
        Files.write(subFailPath, subFailContent.getBytes(UTF_8));

        String subSuccessContent = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_sub_success.dig"), UTF_8);
        Path subSuccessPath = folder.getRoot().toPath().resolve("call_sub_success.dig");
        System.out.println(subSuccessPath.toAbsolutePath().toString());
        Files.write(subSuccessPath, subSuccessContent.getBytes(UTF_8));

        String subNestContent = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_sub_nest.dig"), UTF_8);
        Path subNestPath = folder.getRoot().toPath().resolve("call_sub_nest.dig");
        System.out.println(subNestPath.toAbsolutePath().toString());
        Files.write(subNestPath, subNestContent.getBytes(UTF_8));

        String parentBase = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_nest_retry.dig"), UTF_8);

        //Full path not work in AppVeyor
        //String parentContent = parentBase.replaceFirst("child_path: dummy", "child_path: " + childPath.toString());
        String parentContent = parentBase.replaceFirst("call_sub_success_path: dummy", "call_sub_success_path: " +  "call_sub_success.dig")
                                         .replaceFirst("call_sub_fail_path: dummy", "call_sub_fail_path: " +  "call_sub_fail.dig")
                                         .replaceFirst("call_sub_nest_path: dummy", "call_sub_nest_path: " +  "call_sub_nest.dig");

        Config parent = new YamlConfigLoader().loadString(parentContent).toConfig(ConfigUtils.configFactory);
        runWorkflow("call_nest_retry", parent);
        Path outPath = folder.getRoot().toPath().resolve("out");
        assertThat(new String(Files.readAllBytes(outPath), UTF_8), is("successnestfailedsuccessnestfailedsuccessnestfailed"));
    }

    @Test
    public void retryMainAndNestCall()
            throws Exception
    {
        String subFailContent = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_sub_fail_retry.dig"), UTF_8);
        Path subFailPath = folder.getRoot().toPath().resolve("call_sub_fail_retry.dig");
        System.out.println(subFailPath.toAbsolutePath().toString());
        Files.write(subFailPath, subFailContent.getBytes(UTF_8));

        String subSuccessContent = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_sub_success.dig"), UTF_8);
        Path subSuccessPath = folder.getRoot().toPath().resolve("call_sub_success.dig");
        System.out.println(subSuccessPath.toAbsolutePath().toString());
        Files.write(subSuccessPath, subSuccessContent.getBytes(UTF_8));

        String subNestContent = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_sub_nest.dig"), UTF_8);
        Path subNestPath = folder.getRoot().toPath().resolve("call_sub_nest.dig");
        System.out.println(subNestPath.toAbsolutePath().toString());
        Files.write(subNestPath, subNestContent.getBytes(UTF_8));

        String parentBase = Resources.toString(WorkflowExecutorTest.class.getResource("/io/digdag/core/workflow/call_nest_retry.dig"), UTF_8);

        //Full path not work in AppVeyor
        //String parentContent = parentBase.replaceFirst("child_path: dummy", "child_path: " + childPath.toString());
        String parentContent = parentBase.replaceFirst("call_sub_success_path: dummy", "call_sub_success_path: " +  "call_sub_success.dig")
                                         .replaceFirst("call_sub_fail_path: dummy", "call_sub_fail_path: " +  "call_sub_fail_retry.dig")
                                         .replaceFirst("call_sub_nest_path: dummy", "call_sub_nest_path: " +  "call_sub_nest.dig");

        Config parent = new YamlConfigLoader().loadString(parentContent).toConfig(ConfigUtils.configFactory);
        runWorkflow("call_nest_retry", parent);
        Path outPath = folder.getRoot().toPath().resolve("out");
        assertThat(new String(Files.readAllBytes(outPath), UTF_8), is("successnestfailedfailedfailedsuccessnestfailedfailedfailedsuccessnestfailedfailedfailed"));
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
    public void checkRetry()
            throws Exception
    {
        ProjectStoreManager rm = mock(ProjectStoreManager.class);
        SessionStoreManager sm = mock(SessionStoreManager.class);
        TransactionManager tm = mock(TransactionManager.class);
        TaskQueueDispatcher dispatcher = mock(TaskQueueDispatcher.class);
        WorkflowCompiler compiler = mock(WorkflowCompiler.class);
        DigdagMetrics metrics = StdDigdagMetrics.empty();
        ConfigFactory cf = configFactory;
        ObjectMapper archiveMapper = mock(ObjectMapper.class);
        Config systemConfig = configFactory.create();

        WorkflowExecutor executor = new WorkflowExecutor(rm, sm, tm, dispatcher, compiler, cf, archiveMapper, systemConfig, metrics);
        Config stateParam = cf.create().set("retry_count", "2");
        StoredTask task = mock(StoredTask.class);

        {  // Invalid _retry format. Should return absent.
            Config config = cf.fromJsonString("{\"_retry\":\"${retry_limit}\"}");
            when(task.getConfig()).thenReturn(TaskConfig.validate(config));
            when(task.getStateParams()).thenReturn(stateParam);
            Optional<RetryControl> retryControlOpt = executor.checkRetry(task);
            assertFalse(retryControlOpt.isPresent());
        }
        {  // Valid _retry format and still have a chance to retry.
            Config config = cf.fromJsonString("{\"_retry\":\"3\"}");
            when(task.getConfig()).thenReturn(TaskConfig.validate(config));
            when(task.getStateParams()).thenReturn(stateParam);
            Optional<RetryControl> retryControlOpt = executor.checkRetry(task);
            assertTrue(retryControlOpt.isPresent());
        }
        {  // Valid _retry format but no space to retry.
            Config config = cf.fromJsonString("{\"_retry\":\"2\"}");
            when(task.getConfig()).thenReturn(TaskConfig.validate(config));
            when(task.getStateParams()).thenReturn(stateParam);
            Optional<RetryControl> retryControlOpt = executor.checkRetry(task);
            assertFalse(retryControlOpt.isPresent());
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
    public void ifOperatorFailRetry()
            throws Exception
    {
        runWorkflow("if_operator_fail_retry", loadYamlResource("/io/digdag/core/workflow/if_operator_fail_retry.dig"));
        assertThat(new String(Files.readAllBytes(folder.getRoot().toPath().resolve("out")), UTF_8), is("taskfailedtaskfailedtaskfailedtaskfailed"));
    }

    @Test
    public void ifOperatorFailRetryOnlySub()
            throws Exception
    {
        runWorkflow("if_operator_fail_retry_only_sub", loadYamlResource("/io/digdag/core/workflow/if_operator_fail_retry_only_sub.dig"));
        assertThat(new String(Files.readAllBytes(folder.getRoot().toPath().resolve("out")), UTF_8), is("taskfailedfailedfailedfailed"));
    }

    @Test
    public void forEachRetry()
            throws Exception
    {
        runWorkflow("for_each_retry", loadYamlResource("/io/digdag/core/workflow/for_each_retry.dig"));
        assertThat(new String(Files.readAllBytes(folder.getRoot().toPath().resolve("out")), UTF_8), is("012failed012failed012failed"));
    }

    @Test
    public void ifOperatorDelayedEvalElseDo()
            throws Exception
    {
        runWorkflow("if_operator_else_do", loadYamlResource("/io/digdag/core/workflow/if_operator_else_do.dig"));
        assertThat(new String(Files.readAllBytes(folder.getRoot().toPath().resolve("out")), UTF_8), is("OK_else_do"));

    }

    @Test
    public void randomEnqueue()
            throws Exception
    {
        Config sysConfig = configFactory.create();

        sysConfig.set("executor.enqueue_random_fetch", true);
        sysConfig.set("executor.enqueue_fetch_size", 3);
        DigdagEmbed embed2 = WorkflowTestingUtils.setupEmbed(b -> {
            return b.setSystemConfig(ConfigElement.copyOf(sysConfig));
        });
        try {
            {
                runWorkflow(embed2, "random_enqueue_simple", loadYamlResource("/io/digdag/core/workflow/random_enqueue_simple.dig"));
                Optional<String> result = getResult("out", folder);
                logger.info(result.get());
                assertThat(result.get(), is("step1step2step3"));
            }
            {
                runWorkflow(embed2, "random_enqueue_parallel", loadYamlResource("/io/digdag/core/workflow/random_enqueue_parallel.dig"));
                Optional<String> result = getResult("out", folder);
                assertThat(result.isPresent(), not(false));
                String out = result.get();
                logger.info(out);
                for (int i = 0; i < 8; i++) {
                    assertThat(out, containsString("idx:" + i));
                }
            }
        }
        finally {
            embed2.close();
        }
    }

    private Optional<String> getResult(String fileName, TemporaryFolder folder)
    {
        try {
            return Optional.of(new String(Files.readAllBytes(folder.getRoot().toPath().resolve(fileName)), UTF_8));
        }
        catch (Exception e) {
            e.printStackTrace();
            return Optional.absent();
        }
    }

    private void runWorkflow(String workflowName, Config config)
            throws Exception
    {
        WorkflowTestingUtils.runWorkflow(embed, folder.getRoot().toPath(), workflowName, config);
    }

    private void runWorkflow(DigdagEmbed embed, String workflowName, Config config)
            throws Exception
    {
        WorkflowTestingUtils.runWorkflow(embed, folder.getRoot().toPath(), workflowName, config);
    }
}
