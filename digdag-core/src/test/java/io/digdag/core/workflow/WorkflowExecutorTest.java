package io.digdag.core.workflow;

import java.util.*;
import java.nio.file.Files;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;

import io.digdag.core.database.TransactionManager;
import org.skife.jdbi.v2.IDBI;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.io.Resources;
import io.digdag.core.LocalSite;
import io.digdag.core.archive.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.core.session.*;
import io.digdag.core.workflow.*;
import io.digdag.spi.ScheduleTime;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
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

    private void runWorkflow(String workflowName, Config config)
            throws Exception
    {
        tm.begin(() -> {
            WorkflowTestingUtils.runWorkflow(localSite, folder.getRoot().toPath(), workflowName, config);
            return null;
        });
    }
}
