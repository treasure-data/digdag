package io.digdag.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.workflow.*;
import io.digdag.metrics.DigdagTimed;
import io.digdag.spi.TaskConflictException;
import io.digdag.spi.TaskQueueRequest;
import io.digdag.spi.metrics.DigdagMetrics;
import io.digdag.util.RetryControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.Limits;
import io.digdag.core.agent.AgentId;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.session.ResumingTask;
import io.digdag.core.session.ParameterUpdate;

import io.digdag.core.session.Session;
import io.digdag.core.session.SessionAttempt;
import io.digdag.core.session.SessionControlStore;
import io.digdag.core.session.SessionMonitor;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.StoredTask;
import io.digdag.core.session.Task;
import io.digdag.core.session.TaskAttemptSummary;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.session.TaskStateFlags;

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;

public class WorkflowExecutorMain
{
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutor.class);

    private final ProjectStoreManager rm;
    private final SessionStoreManager sm;
    private final TransactionManager tm;
    private final WorkflowCompiler compiler;
    private final WorkflowExecutor executor;
    private final TaskQueueDispatcher dispatcher;
    private final ConfigFactory cf;
    private final ObjectMapper archiveMapper;
    private final Config systemConfig;
    private final DigdagMetrics metrics;
    private final boolean enqueueRandomFetch;
    private final Integer enqueueFetchSize;


    @Inject
    public WorkflowExecutorMain(
            ProjectStoreManager rm,
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
        this.rm = rm;
        this.sm = sm;
        this.tm = tm;
        this.compiler = compiler;
        this.executor = executor;
        this.dispatcher = dispatcher;
        this.cf = cf;
        this.archiveMapper = archiveMapper;
        this.systemConfig = systemConfig;
        this.metrics = metrics;
        this.enqueueRandomFetch = systemConfig.get("executor.enqueue_random_fetch", Boolean.class, false);
        this.enqueueFetchSize = systemConfig.get("executor.enqueue_fetch_size", Integer.class, 100);
    }

    public void run()
            throws InterruptedException
    {
        runWhile(() -> true);
    }

    public StoredSessionAttemptWithSession runUntilDone(long attemptId)
            throws ResourceNotFoundException, InterruptedException
    {
        try {
            runWhile(() -> {
                try {
                    return !sm.getAttemptStateFlags(attemptId).isDone();
                }
                catch (ResourceNotFoundException ex) {
                    throw Throwables.propagate(ex);
                }
            });
        }
        catch (RuntimeException ex) {
            Throwables.propagateIfInstanceOf(ex.getCause(), ResourceNotFoundException.class);
            throw ex;
        }

        return tm.begin(() -> sm.getAttemptWithSessionById(attemptId), ResourceNotFoundException.class);
    }

    public void runUntilAllDone()
            throws InterruptedException
    {
        runWhile(() -> sm.isAnyNotDoneAttempts());
    }

    private static final int INITIAL_INTERVAL = 100;
    private static final int MAX_INTERVAL = 5000;

    public void runWhile(BooleanSupplier cond)
            throws InterruptedException
    {
        try (WorkflowExecutor.TaskQueuer queuer = new WorkflowExecutor.TaskQueuer()) {
            propagateBlockedChildrenToReady();
            retryRetryWaitingTasks();
            enqueueReadyTasks(queuer);  // TODO enqueue all (not only first 100)
            propagateAllPlannedToDone();
            propagateSessionArchive();

            final AtomicInteger waitMsec = new AtomicInteger(INITIAL_INTERVAL);
            while (true) {
                if (tm.<Boolean>begin(() -> !cond.getAsBoolean())) {
                    break;
                }
                metrics.increment(DigdagMetrics.Category.EXECUTOR, "loopCount");
                //boolean inced = prop.run();
                //boolean retried = retryRetryWaitingTasks();
                //if (inced || retried) {
                //    enqueueReadyTasks(queuer);
                //    propagatorNotice = true;
                //}

                propagateBlockedChildrenToReady();
                retryRetryWaitingTasks();
                enqueueReadyTasks(queuer);

                /**
                 *  propagateSessionArchive() should be always called.
                 *  If propagateSessionArchive() for a session fail,
                 *  next propagateSessionArchive() never call
                 *  until propagateAllPlannedToDone() become true.
                 *  If there is only the session, never archived.
                 *  Checked by WorkflowExecutorCatchingTest.testPropagateSessionArchive()
                 */
                boolean hasModification = propagateAllPlannedToDone();
                propagateSessionArchive();
                if (hasModification) {
                    //propagateSessionArchive();
                }
                else {
                    executor.getPropagatorLock().lock();
                    try {
                        if (executor.isPropagatorNotice()) {
                            executor.setPropagatorNotice(false);
                            waitMsec.set(INITIAL_INTERVAL);
                        }
                        else {
                            metrics.summary(DigdagMetrics.Category.EXECUTOR, "loopWaitMsec", waitMsec.get());
                            boolean noticed = executor.getPropagatorCondition().await(waitMsec.get(), TimeUnit.MILLISECONDS);
                            if (noticed && executor.isPropagatorNotice()) {
                                executor.setPropagatorNotice(false);
                                waitMsec.set(INITIAL_INTERVAL);
                            }
                            else {
                                waitMsec.set(Math.min(waitMsec.get() * 2, MAX_INTERVAL));
                            }
                        }
                    }
                    finally {
                        executor.getPropagatorLock().unlock();
                    }
                }
            }
        }
    }

    @VisibleForTesting
    protected Function<Long, Optional<Boolean>> funcPropagateBlockedChildrenToReady()
    {
        return (pId) ->
                tm.begin(()-> sm.lockTaskIfNotLocked(
                        pId,
                        (store) -> store.trySetChildrenBlockedToReadyOrShortCircuitPlannedOrCanceled(pId) > 0)
                );
    }

    @DigdagTimed(category = "executor", appendMethodName = true)
    protected boolean propagateBlockedChildrenToReady()
    {
        boolean anyChanged = false;
        long lastParentId = 0;
        while (true) {
            long finalLastParentId = lastParentId;
            List<Long> parentIds = tm.begin(() -> sm.findDirectParentsOfBlockedTasks(finalLastParentId));

            if (parentIds.isEmpty()) {
                break;
            }

            anyChanged = parentIds
                    .stream()
                    .map(parentId ->
                            executor.catching(
                                    ()->funcPropagateBlockedChildrenToReady().apply(parentId),
                                    Optional.<Boolean>absent(),
                                    "Failed to set children to ready. paretId:" + parentId
                            )
                                    .or(false)
                    )
                    .reduce(anyChanged, (a, b) -> a || b);
            lastParentId = parentIds.get(parentIds.size() - 1);
        }
        return anyChanged;
    }

    protected Function<Long, Optional<Boolean>> funcSetDoneFromDoneChildren()
    {
        return (tId) ->
                tm.begin(() ->
                        sm.lockTaskIfNotLocked(tId, (store, storedTask) ->
                                setDoneFromDoneChildren(new TaskControl(store, storedTask))));
    }

    @DigdagTimed(category = "executor", appendMethodName = true)
    protected boolean propagateAllPlannedToDone()
    {
        boolean anyChanged = false;
        long lastTaskId = 0;
        while (true) {
            long finalLastTaskId = lastTaskId;
            List<Long> taskIds = tm.begin(() -> sm.findTasksByState(TaskStateCode.PLANNED, finalLastTaskId));
            if (taskIds.isEmpty()) {
                break;
            }
            anyChanged = taskIds
                    .stream()
                    .map(taskId ->
                            executor.catching(
                                    ()->funcSetDoneFromDoneChildren().apply(taskId),
                                    Optional.<Boolean>absent(),
                                    "Failed to call setDoneFromDoneChildren. taskId:" + taskId
                            )
                                    .or(false)
                    )
                    .reduce(anyChanged, (a, b) -> a || b);
            lastTaskId = taskIds.get(taskIds.size() - 1);
        }
        return anyChanged;
    }

    @DigdagTimed(category = "executor", appendMethodName = true)
    protected boolean retryRetryWaitingTasks()
    {
        return tm.begin(() -> sm.trySetRetryWaitingToReady() > 0);
    }

    @VisibleForTesting
    protected Function<TaskAttemptSummary, Optional<Boolean>> funcArchiveTasks()
    {
        return (t) ->
                tm.begin(() ->
                        sm.lockAttemptIfExists(t.getAttemptId(), (store, summary) -> {
                            if (summary.getStateFlags().isDone()) {
                                // already archived. This means that another thread archived
                                // this attempt after findRootTasksByStates call.
                                return false;
                            }
                            else {
                                SessionAttemptControl control = new SessionAttemptControl(store, t.getAttemptId());
                                control.archiveTasks(archiveMapper, t.getState() == TaskStateCode.SUCCESS);
                                return true;
                            }
                        }));
    }

    @DigdagTimed(category = "executor", appendMethodName = true)
    protected boolean propagateSessionArchive()
    {
        boolean anyChanged = false;
        long lastTaskId = 0;
        while (true) {
            long finalLastTaskId = lastTaskId;
            List<TaskAttemptSummary> tasks =
                    tm.begin(() -> sm.findRootTasksByStates(TaskStateCode.doneStates(), finalLastTaskId));
            if (tasks.isEmpty()) {
                break;
            }
            anyChanged = tasks
                    .stream()
                    .map(task ->
                            executor.catching(
                                    ()->funcArchiveTasks().apply(task),
                                    Optional.<Boolean>absent(),
                                    "Failed to call archiveTasks. taskId:" + task.getId()
                            )
                                    .or(false)
                    )
                    .reduce(anyChanged, (a, b) -> a || b);
            lastTaskId = tasks.get(tasks.size() - 1).getId();
        }
        return anyChanged;
    }

    @VisibleForTesting
    protected Function<Long, Boolean> funcEnqueueTask()
    {
        return (tId) ->
                tm.begin(() -> {
                    enqueueTask(dispatcher, tId);
                    return true;
                });
    }

    @DigdagTimed(category = "executor", appendMethodName = true)
    protected void enqueueReadyTasks(WorkflowExecutor.TaskQueuer queuer)
    {
        List<Long> readyTaskIds = tm.begin(() -> sm.findAllReadyTaskIds(enqueueFetchSize, enqueueRandomFetch));
        logger.trace("readyTaskIds:{}", readyTaskIds);
        for (long taskId : readyTaskIds) {  // TODO randomize this result to achieve concurrency
            executor.catching(()->funcEnqueueTask().apply(taskId), true, "Failed to call enqueueTask. taskId:" + taskId);
            //queuer.asyncEnqueueTask(taskId);  // TODO async queuing is probably unnecessary but not sure
        }
    }

    @DigdagTimed(category="executor", appendMethodName = true)
    protected void enqueueTask(final TaskQueueDispatcher dispatcher, final long taskId)
    {
        sm.lockTaskIfNotLocked(taskId, (store, task) -> {
            TaskControl lockedTask = new TaskControl(store, task);
            if (lockedTask.getState() != TaskStateCode.READY) {
                return false;
            }

            if (task.getTaskType().isGroupingOnly()) {
                return retryGroupingTask(lockedTask);
            }

            if (task.getStateFlags().isCancelRequested()) {
                return lockedTask.setToCanceled();
            }

            int siteId;
            try {
                siteId = sm.getSiteIdOfTask(taskId);
            }
            catch (ResourceNotFoundException ex) {
                tm.reset();
                Exception error = new IllegalStateException("Task id="+taskId+" is ready to run but associated session attempt does not exist.", ex);
                logger.error("Database state error enqueuing task.", error);
                return false;
            }

            try {
                // TODO make queue name configurable. note that it also needs a new REST API and/or
                //      CLI ccommands to create/delete/manage queues.
                Optional<String> queueName = Optional.absent();

                String encodedUnique = encodeUniqueQueuedTaskName(lockedTask.get());

                TaskQueueRequest request = TaskQueueRequest.builder()
                        .priority(0)  // TODO make this configurable
                        .uniqueName(encodedUnique)
                        .data(Optional.absent())
                        .build();

                logger.debug("Queuing task of attempt_id={}: id={} {}", task.getAttemptId(), task.getId(), task.getFullName());
                try {
                    dispatcher.dispatch(siteId, queueName, request);
                }
                catch (TaskConflictException ex) {
                    tm.reset();
                    logger.warn("Task name {} is already queued in queue={} of site id={}. Skipped enqueuing",
                            encodedUnique, queueName.or("<shared>"), siteId);
                }

                ////
                // don't throw exceptions after here. task is already dispatched to a queue
                //

                boolean updated = lockedTask.setReadyToRunning();
                if (!updated) {
                    // return value of setReadyToRunning must be true because this task is locked
                    // (won't be updated by other machines concurrently) and confirmed that
                    // current state is READY.
                    logger.warn("Unexpected state change failure from READY to RUNNING: {}", task);
                }

                return updated;
            }
            catch (Exception ex) {
                tm.reset();
                logger.error("Enqueue error, making this task failed: {}", task, ex);
                // TODO retry here?
                return executor.taskFailed(lockedTask,
                        buildExceptionErrorConfig(ex).toConfig(cf));
            }
        }).or(false);
    }

    private boolean retryGroupingTask(TaskControl lockedTask)
    {
        // rest task state of subtasks
        StoredTask task = lockedTask.get();

        TaskTree tree = new TaskTree(sm.getTaskRelations(task.getAttemptId()));
        List<Long> childrenIdList = tree.getRecursiveChildrenIdList(task.getId());
        lockedTask.copyInitialTasksForRetry(task.getFullName(), childrenIdList);

        lockedTask.setGroupRetryReadyToPlanned();

        return true;
    }

    private boolean setDoneFromDoneChildren(TaskControl lockedTask)
    {
        if (lockedTask.getState() != TaskStateCode.PLANNED) {
            return false;
        }
        if (lockedTask.isAnyProgressibleChild()) {
            return false;
        }

        logger.trace("setDoneFromDoneChildren {}", lockedTask.get());

        StoredTask task = lockedTask.get();
        // this parent task must not be "SUCCESS" when a child is canceled.
        // here assumes that CANCEL_REQUESTED flag is set to all tasks of a session
        // transactionally. If CANCEL_REQUESTED flag is set to a child,
        // CANCEL_REQUESTED flag should also be set to this parent task.
        if (task.getStateFlags().isCancelRequested()) {
            return lockedTask.setToCanceled();
        }

        if (task.getStateFlags().isDelayedError()) {
            // DELAYED_ERROR
            // this error was formerly delayed by taskFailed
            boolean updated = lockedTask.setPlannedToError();
            if (!updated) {
                // return value of setPlannedToError must be true because this task is locked
                // (won't be updated by other machines concurrently) and confirmed that
                // current state is PLANNED.
                logger.warn("Unexpected state change failure from PLANNED to ERROR: {}", task);
            }
            return updated;
        }
        else if (task.getStateFlags().isDelayedGroupError()) {
            // DELAYED_GROUP_ERROR
            // this error was formerly delayed by last setDoneFromDoneChildren call
            boolean updated = lockedTask.setPlannedToGroupError();
            if (!updated) {
                // return value of setPlannedToGroupError must be true because this task is locked
                // (won't be updated by other machines concurrently) and confirmed that
                // current state is PLANNED.
                logger.warn("Unexpected state change failure from PLANNED to GROUP_ERROR: {}", task);
            }
            return updated;
        }
        else if (lockedTask.isAnyErrorChild()) {
            // group error
            boolean updated;

            Optional<RetryControl> retryControlOpt = checkRetry(task);
            if (retryControlOpt.isPresent()) {
                RetryControl retryControl = retryControlOpt.get();
                updated = lockedTask.setPlannedToGroupRetryWaiting(
                        retryControl.getNextRetryStateParams(),
                        retryControl.getNextRetryInterval());
            }
            else {
                List<Long> errorTaskIds = new ArrayList<>();

                // root task is always group-only task
                boolean isRootTask = !lockedTask.get().getParentId().isPresent();
                if (isRootTask) {
                    errorTaskIds.add(addAttemptFailureAlertTask(lockedTask));
                }

                try {
                    Optional<Long> errorTask = executor.addErrorTasksIfAny(lockedTask,
                            true,
                            (export) -> {
                                collectErrorParams(export, lockedTask.get());
                                return export;
                            });
                    if (errorTask.isPresent()) {
                        errorTaskIds.add(errorTask.get());
                    }
                }
                catch (TaskLimitExceededException ex) {
                    tm.reset();
                    logger.warn("Failed to add error tasks because of task limit");
                    // addErrorTasksIfAny threw TaskLimitExceededException. Giveup adding them.
                    // TODO this is a fundamental design problem. Probably _error tasks should be removed.
                    //      use notification tasks instead.
                }
                catch (ConfigException ex) {
                    // Workflow config has a problem (e.g. more than one operator). Nothing we can do here.
                    // WorkflowCompiler should be validation so that this error doesn't happen.
                    // TODO this is a fundamental design problem. Probably _error tasks should be removed.
                    //      use notification tasks instead.
                    logger.warn("Found a broken _error task in attempt {} task {}. Skipping this task.",
                            task.getAttemptId(), task.getId(), ex);
                }

                if (errorTaskIds.isEmpty()) {
                    // set GROUP_ERROR state
                    updated = lockedTask.setPlannedToGroupError();
                }
                else {
                    // set DELAYED_GROUP_ERROR flag and keep state. actual change of state is delay
                    // until next setDoneFromDoneChildren call.
                    updated = lockedTask.setPlannedToPlannedWithDelayedGroupError();
                }
            }

            return updated;
        }
        else {
            boolean updated = lockedTask.setPlannedToSuccess();
            if (!updated) {
                // return value of setPlannedToSuccess must be true because this task is locked
                // (won't be updated by other machines concurrently) and confirmed that
                // current state is PLANNED.
                logger.warn("Unexpected state change failure from PLANNED to SUCCESS: {}", task);
            }
            return updated;
        }
    }

    private void collectErrorParams(Config params, StoredTask task)
    {
        List<Long> childrenFromThis;
        {
            TaskTree tree = new TaskTree(sm.getTaskRelations(task.getAttemptId()));
            childrenFromThis = tree.getRecursiveChildrenIdList(task.getId());
        }

        // merge store params to export params
        List<ParameterUpdate> childrenStoreParams = sm.getStoreParams(childrenFromThis);
        for (ParameterUpdate childStoreParams : childrenStoreParams) {
            childStoreParams.applyTo(params);
        }

        // merge all error params
        Config error = cf.create();
        {
            List<Config> childrenErrors = sm.getErrors(childrenFromThis);
            for (Config childError : childrenErrors) {
                error.merge(childError);
            }
        }
        params.set("error", error);
    }

    /**
     * Check retriable of task
     * @param task
     * @return if present(), should retry, if absent() should not retry.
     */
    Optional<RetryControl> checkRetry(StoredTask task)
    {
        try {
            RetryControl retryControl = RetryControl.prepare(task.getConfig().getMerged(), task.getStateParams(), false);  // don't retry by default
            if (retryControl.evaluate()) {
                return Optional.of(retryControl);
            }
            else {
                return Optional.absent();
            }
        }
        catch (ConfigException ce) {
            logger.warn("Ignore retry parameter because of invalid retry configuration. attempt_id:{} config:{}", task.getAttemptId(), task.getConfig());
            return Optional.absent();
        }
    }

    private long addAttemptFailureAlertTask(TaskControl rootTask)
    {
        Config config = cf.create();
        config.set("_type", "notify");
        config.set("_command", "Workflow session attempt failed");
        WorkflowTaskList tasks = compiler.compileTasks(rootTask.get().getFullName(), "^failure-alert", config);
        return rootTask.addGeneratedSubtasksWithoutLimit(tasks, ImmutableList.of(), false);
    }


    private static String encodeUniqueQueuedTaskName(StoredTask task)
    {
        int retryCount = task.getRetryCount();
        if (retryCount == 0) {
            return Long.toString(task.getId());
        }
        else {
            return Long.toString(task.getId()) + ".r" + Integer.toString(retryCount);
        }
    }


}
