package io.digdag.core.workflow;

import io.digdag.client.config.Config;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.LocalSite;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import java.nio.file.Path;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static io.digdag.core.workflow.WorkflowTestingUtils.runWorkflow;
import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class StoreParameterTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DigdagEmbed embed;
    private LocalSite localSite;
    private Path projectPath;

    @Before
    public void setUp()
        throws Exception
    {
        this.embed = setupEmbed();
        this.localSite = embed.getInjector().getInstance(LocalSite.class);
        this.projectPath = folder.newFolder().toPath();
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
        StoredSessionAttemptWithSession attempt = runWorkflow(localSite, projectPath, "store", loadYamlResource("/io/digdag/core/workflow/store.dig"));

        List<ArchivedTask> tasks = localSite.getSessionStore().getTasksOfAttempt(attempt.getId());

        for (ArchivedTask task : tasks) {
            System.out.println("task : " + task);
        }

        assertThat(attempt.getStateFlags().isSuccess(), is(true));

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
