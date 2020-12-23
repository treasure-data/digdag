package io.digdag.core.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.Limits;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.TaskAttemptSummary;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.metrics.DigdagMetrics;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

import static io.digdag.core.workflow.WorkflowTestingUtils.loadYamlResource;
import static io.digdag.core.workflow.WorkflowTestingUtils.runWorkflow;

import static org.junit.Assert.assertEquals;

public class WorkflowExecutorCatchingTest
{
    DigdagEmbed digdag = null;
    WorkflowExecutorWithArbitraryErrors executor;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        digdag = setupEmbed();
        executor = (WorkflowExecutorWithArbitraryErrors)digdag.getInjector().getInstance(WorkflowExecutor.class);
    }

    @After
    public void finish() throws Exception
    {
        if (digdag != null) {
            digdag.close();
            digdag = null;
        }
    }

    private DigdagEmbed setupEmbed()
    {
        return WorkflowTestingUtils.setupEmbed((e) ->
            e.overrideModulesWith((binder) ->
                binder.bind(WorkflowExecutor.class).to(WorkflowExecutorWithArbitraryErrors.class).in(Scopes.SINGLETON)
            )
        );
    }

    @Test
    public void testEnqueueTask() throws Exception
    {
        executor.setFuncEnqueueTaskFailNumber(2); // No 2 task will fail.
        runWorkflow(digdag, folder.getRoot().toPath(), "basic", loadYamlResource("/io/digdag/core/workflow/catching.dig"));
        // catching.dig has 3 tasks. But 2nd task failed and retried, the total enqueue tasks will be 4.
        assertEquals(4, executor.getFuncEnqueueTaskCounter());
        assertEquals(1, executor.getCatchingCounter());
    }

    @Test
    public void testPropagateBlockedChildrenToReady() throws Exception
    {
        executor.setFuncPropagateBlockedChildrenToReadyFailNumber(1);
        runWorkflow(digdag, folder.getRoot().toPath(), "basic", loadYamlResource("/io/digdag/core/workflow/catching.dig"));
        assertEquals(1, executor.getCatchingCounter());
    }

    @Test
    public void testPropagateAllPlannedToDone() throws Exception
    {
        executor.setFuncSetDoneFromDoneChildrenFailNumber(1);
        runWorkflow(digdag, folder.getRoot().toPath(), "basic", loadYamlResource("/io/digdag/core/workflow/catching.dig"));
        assertEquals(1, executor.getCatchingCounter());
    }

    @Test
    public void testPropagateSessionArchive() throws Exception
    {
        executor.setFuncArchiveTasksFailNumber(1);
        runWorkflow(digdag, folder.getRoot().toPath(), "basic", loadYamlResource("/io/digdag/core/workflow/catching.dig"));
        assertEquals(1, executor.getCatchingCounter());
    }


    /**
     *  This executor will fail in some method with exception dependsOn funcXXXXFailNumber members.
     *  Even though unexpected exceptions happen, catching() catch it safely and the executor process will proceed
     */
    public static class WorkflowExecutorWithArbitraryErrors extends WorkflowExecutor
    {
        private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutorWithArbitraryErrors.class);

        private int funcEnqueueTaskFailNumber = 0; //The nth enqueued task will be failed intentionally.
        private int funcEnqueueTaskCounter = 0;

        private int funcPropagateBlockedChildrenToReadyFailNumber = 0;
        private int funcPropagateBlockedChildrenToReadyCounter = 0;

        private int funcSetDoneFromDoneChildrenFailNumber = 0;
        private int funcSetDoneFromDoneChildrenCounter = 0;

        private int funcArchiveTasksFailNumber = 0;
        private int funcArchiveTasksCounter = 0;

        private int catchingCounter = 0;

        @Override
        public void catchingNotify(Exception e)
        {
            catchingCounter++;
        }

        @Inject
        public WorkflowExecutorWithArbitraryErrors(ProjectStoreManager rm,
                                                   SessionStoreManager sm,
                                                   TransactionManager tm,
                                                   TaskQueueDispatcher dispatcher,
                                                   WorkflowCompiler compiler,
                                                   ConfigFactory cf,
                                                   ObjectMapper archiveMapper,
                                                   Config systemConfig,
                                                   DigdagMetrics metrics,
                                                   Limits limits)
        {
            super(rm, sm, tm, dispatcher, compiler, cf, archiveMapper, systemConfig, limits, metrics);
        }

        @Override
        protected Function<Long, Boolean> funcEnqueueTask()
        {
            funcEnqueueTaskCounter++;
            logger.debug("funcEnqueueTask called:" + funcEnqueueTaskCounter);
            if (funcEnqueueTaskCounter == funcEnqueueTaskFailNumber) {
                logger.info("funcEnqueueTask() throw Exception. counter=" + funcEnqueueTaskCounter);
                throw new RuntimeException("Unknown exception");
            }
            return super.funcEnqueueTask();
        }

        @Override
        protected Function<Long, Optional<Boolean>> funcPropagateBlockedChildrenToReady()
        {
            funcPropagateBlockedChildrenToReadyCounter++;
            if (funcPropagateBlockedChildrenToReadyCounter == funcPropagateBlockedChildrenToReadyFailNumber) {
                logger.info("funcPropagateBlockedChildrenToReady throw Exception. counter=" + funcPropagateBlockedChildrenToReadyCounter);
                throw new RuntimeException("Unknown exception");
            }
            return super.funcPropagateBlockedChildrenToReady();
        }

        @Override
        protected Function<Long, Optional<Boolean>> funcSetDoneFromDoneChildren()
        {
            funcSetDoneFromDoneChildrenCounter++;
            if (funcSetDoneFromDoneChildrenCounter == funcSetDoneFromDoneChildrenFailNumber) {
                logger.info("funcSetDoneFromDoneChildren throw Exception. counter=" + funcSetDoneFromDoneChildrenCounter);
                throw new RuntimeException("Unknown exception");
            }
            return super.funcSetDoneFromDoneChildren();
        }

        @Override
        protected Function<TaskAttemptSummary, Optional<Boolean>> funcArchiveTasks()
        {
            funcArchiveTasksCounter++;
            if (funcArchiveTasksCounter == funcArchiveTasksFailNumber) {
                logger.info("funcArchiveTasks throw Exception. counter=" + funcArchiveTasksCounter);
                throw new RuntimeException("Unknown exception");
            }
            return super.funcArchiveTasks();
        }

        public void setFuncEnqueueTaskFailNumber(int v) { this.funcEnqueueTaskFailNumber = v; }

        public void setFuncPropagateBlockedChildrenToReadyFailNumber(int v)
        {
            this.funcPropagateBlockedChildrenToReadyFailNumber = v;
        }

        public void setFuncSetDoneFromDoneChildrenFailNumber(int v)
        {
            this.funcSetDoneFromDoneChildrenFailNumber = v;
        }

        public void setFuncArchiveTasksFailNumber(int v)
        {
            this.funcArchiveTasksFailNumber = v;
        }

        public int getFuncEnqueueTaskCounter() { return funcEnqueueTaskCounter; }

        public int getFuncPropagateBlockedChildrenToReadyCounter() { return funcPropagateBlockedChildrenToReadyCounter; }

        public int getCatchingCounter() { return catchingCounter; }

    }


}
