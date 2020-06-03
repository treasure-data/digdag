package io.digdag.core.workflow;

import io.digdag.client.config.Config;
import io.digdag.server.DigdagEmbed;
import io.digdag.server.LocalSite;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import java.nio.file.Path;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static io.digdag.core.workflow.WorkflowTestingUtils.runWorkflow;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class StoreParameterTest
{
    private static DigdagEmbed embed;

    @BeforeClass
    public static void createDigdagEmbed()
            throws Exception
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
    private Path projectPath;
    private TransactionManager tm;

    @Before
    public void setUp()
        throws Exception
    {
        this.localSite = embed.getLocalSite();
        this.tm = embed.getTransactionManager();
        this.projectPath = folder.newFolder().toPath();
    }

    @Test
    public void run()
        throws Exception
    {
        StoredSessionAttemptWithSession attempt =
                runWorkflow(embed, projectPath, "store", loadYamlResource("/io/digdag/core/workflow/store.dig"));

        tm.begin(() -> {
            assertThat(attempt.getStateFlags().isSuccess(), is(true));

            List<ArchivedTask> tasks = localSite.getSessionStore().getTasksOfAttempt(attempt.getId());

            assertStoreParams(tasks, "+verify1", newConfig()
                    .set("verify1_text", "old")
            );
            assertStoreParams(tasks, "+verify2", newConfig()
                    .set("verify2_override", "new")
                    .set("verify2_merge", "merged")
                    .set("verify2_merge_nested", "nested")
                    .set("verify2_reset", "true")
            );
            assertStoreParams(tasks, "+verify3", newConfig()
                    .set("verify3_reset", "")
                    .set("verify3_reset_set", "set")
                    .set("verify3_keep_nested", "kept")
                    .set("verify3_reset_nested", "")
                    .set("verify3_merge_nested", "new")
            );
            return null;
        });
    }

    private void assertStoreParams(List<ArchivedTask> tasks, String taskName, Config expected)
    {
        ArchivedTask task = tasks.stream()
            .filter(t -> t.getFullName().endsWith(taskName))
            .findFirst()
            .orElseThrow(() -> new NullPointerException(taskName));

        assertThat(task.getStoreParams(), is(expected));
    }
}
