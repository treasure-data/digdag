package io.digdag.core.workflow;

import java.util.*;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;
import org.skife.jdbi.v2.IDBI;
import org.junit.*;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.io.Resources;
import io.digdag.core.LocalSite;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.core.session.*;
import io.digdag.core.workflow.*;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.spi.ScheduleTime;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import static java.nio.charset.StandardCharsets.UTF_8;
import static io.digdag.core.workflow.WorkflowTestingUtils.*;
import static org.junit.Assert.*;

public class WorkflowExecutorCasesTest
{
    private DigdagEmbed embed;

    @Before
    public void setUp()
        throws Exception
    {
        embed = setupEmbed();
    }

    @After
    public void destroy()
        throws Exception
    {
        embed.destroy();
    }

    @Test
    public void run()
        throws Exception
    {
        runWorkflow("+basic", loadYamlResource("/digdag/workflow/cases/basic.yml"));
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

    private void runWorkflow(String name, Config config)
        throws InterruptedException
    {
        try {
            LocalSite localSite = embed.getLocalSite();
            LocalSite.StoreWorkflowResult stored = localSite.storeLocalWorkflowsWithoutSchedule(
                    "defualt",
                    "revision-" + UUID.randomUUID(),
                    Dagfile.fromConfig(config).toArchiveMetadata(ZoneId.of("UTC")));
            StoredWorkflowDefinition def = findDefinition(stored.getWorkflowDefinitions(), name);
            AttemptRequest ar = localSite.getAttemptBuilder()
                .buildFromStoredWorkflow(
                        stored.getRevision(),
                        def,
                        config.getFactory().create(),
                        ScheduleTime.runNow(Instant.ofEpochSecond(Instant.now().getEpochSecond())),
                        Optional.absent());
            StoredSessionAttemptWithSession attempt = localSite.submitWorkflow(ar, def);
            localSite.runUntilDone(attempt.getId());
        }
        catch (ResourceNotFoundException | ResourceConflictException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private StoredWorkflowDefinition findDefinition(List<StoredWorkflowDefinition> defs, String name)
    {
        for (StoredWorkflowDefinition def : defs) {
            if (def.getName().equals(name)) {
                return def;
            }
        }
        throw new RuntimeException("Workflow does not exist: " + name);
    }
}
