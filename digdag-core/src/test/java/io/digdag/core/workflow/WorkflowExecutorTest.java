package io.digdag.core.workflow;

import java.nio.file.Files;

import io.digdag.core.database.TransactionManager;
import io.digdag.core.LocalSite;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.DigdagEmbed;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.is;

public class WorkflowExecutorTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DigdagEmbed embed;
    private LocalSite localSite;
    private TransactionManager tm;

    @Before
    public void setUp()
        throws Exception
    {
        this.embed = setupEmbed();
        this.localSite = embed.getLocalSite();
        this.tm = embed.getTransactionManager();
    }

    @After
    public void destroy()
        throws Exception
    {
        embed.close();
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
        assertThat(new String(Files.readAllBytes(folder.getRoot().toPath().resolve("out")), UTF_8), is("trytrytrytry"));
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
