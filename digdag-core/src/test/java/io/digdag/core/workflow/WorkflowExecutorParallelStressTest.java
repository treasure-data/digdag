package io.digdag.core.workflow;

import io.digdag.client.config.Config;
import io.digdag.core.LocalSite;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.workflow.WorkflowTestingUtils;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static io.digdag.core.workflow.WorkflowTestingUtils.setupEmbed;
import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;

public class WorkflowExecutorParallelStressTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private DigdagEmbed embed;
    private LocalSite localSite;

    @Before
    public void setUp()
        throws Exception
    {
        this.embed = setupEmbed();
        this.localSite = embed.getInjector().getInstance(LocalSite.class);
    }

    @After
    public void destroy()
        throws Exception
    {
        embed.close();
    }

    @Test
    public void parallelExecutionLoopShouldNotThrowError()
        throws Exception
    {
        StoredSessionAttemptWithSession attempt = submitWorkflow("parallel_stress", loadYamlResource("/io/digdag/core/workflow/parallel_stress.dig"));

        ExecutorService threads = Executors.newCachedThreadPool();
        ImmutableList.Builder<Future> futures = ImmutableList.builder();

        for (int i = 0; i < 20; i++) {
            futures.add(threads.submit(() -> {
                try {
                    localSite.runUntilDone(attempt.getId());
                }
                catch (Exception ex) {
                    throw Throwables.propagate(ex);
                }
            }));
        }

        for (Future f : futures.build()) {
            f.get();
        }
    }

    private StoredSessionAttemptWithSession submitWorkflow(String workflowName, Config config)
        throws ResourceNotFoundException, ResourceConflictException
    {
        return WorkflowTestingUtils.submitWorkflow(localSite, folder.getRoot().toPath(), workflowName, config);
    }
}
