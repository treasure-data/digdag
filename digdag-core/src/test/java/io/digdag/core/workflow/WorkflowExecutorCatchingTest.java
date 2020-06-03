package io.digdag.core.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.server.DigdagEmbed;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.TaskAttemptSummary;
import io.digdag.executor.WorkflowExecutorMain;
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
    WorkflowExecutorMainWithArbitraryErrors executorMain;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        digdag = setupEmbed();
        executorMain = (WorkflowExecutorMainWithArbitraryErrors)digdag.getInjector().getInstance(WorkflowExecutorMain.class);
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
            e.overrideModulesWith((binder) -> {
                        binder.bind(WorkflowExecutor.class).to(WorkflowExecutorWithCatchingCounter.class).in(Scopes.SINGLETON);
                        binder.bind(WorkflowExecutorMain.class).to(WorkflowExecutorMainWithArbitraryErrors.class).in(Scopes.SINGLETON);
                    }
            )
        );
    }

    @Test
    public void testEnqueueTask() throws Exception
    {
        executorMain.setFuncEnqueueTaskFailNumber(2); // No 2 task will fail.
        runWorkflow(digdag, folder.getRoot().toPath(), "basic", loadYamlResource("/io/digdag/core/workflow/catching.dig"));
        // catching.dig has 3 tasks. But 2nd task failed and retried, the total enqueue tasks will be 4.
        assertEquals(4, executorMain.getFuncEnqueueTaskCounter());
        assertEquals(1, executorMain.getCatchingCounter());
    }

    @Test
    public void testPropagateBlockedChildrenToReady() throws Exception
    {
        executorMain.setFuncPropagateBlockedChildrenToReadyFailNumber(1);
        runWorkflow(digdag, folder.getRoot().toPath(), "basic", loadYamlResource("/io/digdag/core/workflow/catching.dig"));
        assertEquals(1, executorMain.getCatchingCounter());
    }

    @Test
    public void testPropagateAllPlannedToDone() throws Exception
    {
        executorMain.setFuncSetDoneFromDoneChildrenFailNumber(1);
        runWorkflow(digdag, folder.getRoot().toPath(), "basic", loadYamlResource("/io/digdag/core/workflow/catching.dig"));
        assertEquals(1, executorMain.getCatchingCounter());
    }

    @Test
    public void testPropagateSessionArchive() throws Exception
    {
        executorMain.setFuncArchiveTasksFailNumber(1);
        runWorkflow(digdag, folder.getRoot().toPath(), "basic", loadYamlResource("/io/digdag/core/workflow/catching.dig"));
        assertEquals(1, executorMain.getCatchingCounter());
    }


    public static class WorkflowExecutorWithCatchingCounter extends WorkflowExecutor
    {
        private int catchingCounter = 0;

        @Inject
        public WorkflowExecutorWithCatchingCounter(ProjectStoreManager rm,
                                                       SessionStoreManager sm,
                                                       TransactionManager tm,
                                                       TaskQueueDispatcher dispatcher,
                                                       WorkflowCompiler compiler,
                                                       ConfigFactory cf,
                                                       ObjectMapper archiveMapper,
                                                       Config systemConfig,
                                                       DigdagMetrics metrics)
        {
            super(rm, sm, tm, dispatcher, compiler, cf, archiveMapper, systemConfig, metrics);
        }

        @Override
        public void catchingNotify(Exception e)
        {
            catchingCounter++;
        }

        public int getCatchingCounter() { return catchingCounter; }

    }
    /**
     *  This executor will fail in some method with exception dependsOn funcXXXXFailNumber members.
     *  Even though unexpected exceptions happen, catching() catch it safely and the executor process will proceed
     */
    public static class WorkflowExecutorMainWithArbitraryErrors extends WorkflowExecutorMain
    {
        private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutorMainWithArbitraryErrors.class);
        private WorkflowExecutorWithCatchingCounter executor;

        private int funcEnqueueTaskFailNumber = 0; //The nth enqueued task will be failed intentionally.
        private int funcEnqueueTaskCounter = 0;

        private int funcPropagateBlockedChildrenToReadyFailNumber = 0;
        private int funcPropagateBlockedChildrenToReadyCounter = 0;

        private int funcSetDoneFromDoneChildrenFailNumber = 0;
        private int funcSetDoneFromDoneChildrenCounter = 0;

        private int funcArchiveTasksFailNumber = 0;
        private int funcArchiveTasksCounter = 0;

        @Inject
        public WorkflowExecutorMainWithArbitraryErrors(ProjectStoreManager rm,
                                                       SessionStoreManager sm,
                                                       TransactionManager tm,
                                                       TaskQueueDispatcher dispatcher,
                                                       WorkflowCompiler compiler,
                                                       WorkflowExecutor executor,
                                                       ConfigFactory cf,
                                                       ObjectMapper archiveMapper,
                                                       Config systemConfig,
                                                       DigdagMetrics metrics)
        {
            super(rm, sm, tm, dispatcher, compiler, executor, cf, archiveMapper, systemConfig, metrics);
            this.executor = (WorkflowExecutorWithCatchingCounter)executor;
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

        public int getCatchingCounter() { return executor.getCatchingCounter(); }

    }


}
