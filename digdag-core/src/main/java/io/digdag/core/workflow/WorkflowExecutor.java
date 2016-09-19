package io.digdag.core.workflow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.agent.AgentId;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.session.ResumingTask;
import io.digdag.core.session.Session;
import io.digdag.core.session.SessionAttempt;
import io.digdag.core.session.SessionMonitor;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.StoredTask;
import io.digdag.core.session.Task;
import io.digdag.core.session.TaskAttemptSummary;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.session.TaskStateFlags;
import io.digdag.core.session.TaskStateSummary;
import io.digdag.spi.Notifier;
import io.digdag.spi.TaskQueueLock;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TaskQueueRequest;
import io.digdag.spi.TaskConflictException;
import io.digdag.spi.TaskNotFoundException;
import io.digdag.util.RetryControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static java.util.Locale.ENGLISH;

/**
 * State transitions.
 *
 * BLOCKED:
 *   propagateAllBlockedToReady:
 *     store.trySetChildrenBlockedToReadyOrShortCircuitPlannedOrCanceled:
 *       (if GROUPING_ONLY flag is set) : PLANNED
 *       (if CANCEL_REQUESTED flag is set) : CANCELED
 *       : READY
 *
 * READY:
 *   enqueueReadyTasks:
 *     enqueueTask:
 *       (if CANCEL_REQUESTED flag is set) lockedTask.setToCanceled:
 *         : CANCELED
 *       lockedTask.setReadyToRunning:
 *         : RUNNING
 *   NOTE: because updated_at column is used as a part of identifier of
 *         queued task, updated_at must not be updated when state is READY
 *         so that enqueueReadyTasks can detect duplicated enqueuing.
 *
 * RUNNING:
 *   taskFailed:
 *     (if retryInterval is set) lockedTask.setRunningToRetryWaiting:
 *       : RETRY_WAITING with error
 *     (if error task exists) lockedTask.setRunningToPlannedWithDelayedError:
 *       : PLANNED with error and DELAYED_ERROR flag
 *     lockedTask.setRunningToShortCircuitError:
 *       : ERROR with error
 *
 *   taskSucceeded:
 *     lockedTask.setRunningToPlannedSuccessful:
 *       : PLANNED
 *
 *   retryTask:
 *     lockedTask.setRunningToRetryWaiting:
 *       : RETRY_WAITING
 *
 * RETRY_WAITING
 *   retryRetryWaitingTasks:
 *     sm.trySetRetryWaitingToReady:
 *       : READY
 *
 * GROUP_RETRY_WAITING
 *   retryRetryWaitingTasks:
 *     sm.trySetRetryWaitingToReady:
 *       : READY
 *
 * PLANNED:
 *   setDoneFromDoneChildren:
 *     (if all children are not progressible):
 *       (if CANCEL_REQUESTED flag is set) lockedTask.setToCanceled:
 *         : CANCELED
 *       (if DELAYED_ERROR flag is set):
 *         (TODO: recovery)
 *         lockedTask.setPlannedToError:
 *           : ERROR
 *       (if DELAYED_GROUP_ERROR flag is set) lockedTask.setPlannedToGroupError:
 *         (TODO: recovery)
 *         : GROUP_ERROR
 *       (if a child with ERROR or GROUP_ERROR state exists):
 *         (if retry option is set) lockedTask.setPlannedToGroupRetryWaiting:
 *           : GROUP_RETRY_WAITING  // TODO this state needs to re-submit child tasks with BLOCKED state
 *         (if error task exists) lockedTask.setPlannedToPlannedWithDelayedGroupError
 *           : PLANNED with DELAYED_GROUP_ERROR flag
 *         lockedTask.setPlannedToGroupError
 *           : GROUP_ERROR
 *       lockedTask.setPlannedToSuccess:
 *         : SUCCESS
 *
 * GROUP_ERROR:
 *   not progressible
 *
 * SUCCESS:
 *   not progressible
 *
 * ERROR:
 *   not progressible
 *
 * CANCELED:
 *   not progressible
 *
 */
public class WorkflowExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutor.class);

    private final ProjectStoreManager rm;
    private final SessionStoreManager sm;
    private final WorkflowCompiler compiler;
    private final TaskQueueDispatcher dispatcher;
    private final ConfigFactory cf;
    private final ObjectMapper archiveMapper;
    private final Config systemConfig;
    private Notifier notifier;

    private final Lock propagatorLock = new ReentrantLock();
    private final Condition propagatorCondition = propagatorLock.newCondition();
    private volatile boolean propagatorNotice = false;

    @Inject
    public WorkflowExecutor(
            ProjectStoreManager rm,
            SessionStoreManager sm,
            TaskQueueDispatcher dispatcher,
            WorkflowCompiler compiler,
            ConfigFactory cf,
            ObjectMapper archiveMapper,
            Config systemConfig,
            Notifier notifier)
    {
        this.rm = rm;
        this.sm = sm;
        this.compiler = compiler;
        this.dispatcher = dispatcher;
        this.cf = cf;
        this.archiveMapper = archiveMapper;
        this.systemConfig = systemConfig;
        this.notifier = notifier;
    }

    public StoredSessionAttemptWithSession submitWorkflow(int siteId,
            AttemptRequest ar,
            WorkflowDefinition def)
        throws ResourceNotFoundException, SessionAttemptConflictException
    {
        Workflow workflow = compiler.compile(def.getName(), def.getConfig());  // TODO cache (CachedWorkflowCompiler which takes def id as the cache key)
        WorkflowTaskList tasks = workflow.getTasks();

        return submitTasks(siteId, ar, tasks);
    }

    private static final DateTimeFormatter SESSION_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxx", ENGLISH);

    public StoredSessionAttemptWithSession submitTasks(int siteId, AttemptRequest ar,
            WorkflowTaskList tasks)
        throws ResourceNotFoundException, SessionAttemptConflictException
    {
        if (logger.isTraceEnabled()) {
            for (WorkflowTask task : tasks) {
                logger.trace("  Step[{}]: {}", task.getIndex(), task.getName());
                logger.trace("    parent: {}", task.getParentIndex().transform(it -> Integer.toString(it)).or("(root)"));
                logger.trace("    upstreams: {}", task.getUpstreamIndexes().stream().map(it -> Integer.toString(it)).collect(Collectors.joining(", ")));
                logger.trace("    config: {}", task.getConfig());
            }
        }

        int projId = ar.getStored().getProjectId();
        Session session = Session.of(projId, ar.getWorkflowName(), ar.getSessionTime());

        SessionAttempt attempt = SessionAttempt.of(
                ar.getRetryAttemptName(),
                ar.getSessionParams(),
                ar.getTimeZone(),
                Optional.of(ar.getStored().getWorkflowDefinitionId()));

        TaskConfig.validateAttempt(attempt);

        final WorkflowTask root = tasks.get(0);

        List<ResumingTask> resumingTasks;
        if (ar.getResumingAttemptId().isPresent()) {
            resumingTasks = TaskControl.buildResumingTaskMap(
                    sm.getSessionStore(siteId),
                    ar.getResumingAttemptId().get(),
                    ar.getResumingTasks());
            for (ResumingTask resumingTask : resumingTasks) {
                if (resumingTask.getFullName().equals(root.getFullName())) {
                    throw new IllegalResumeException("Resuming root task is not allowed");
                }
            }
        }
        else {
            resumingTasks = ImmutableList.of();
        }

        TaskStateCode rootTaskState = root.getTaskType().isGroupingOnly()
                ? TaskStateCode.PLANNED
                : TaskStateCode.READY;

        StoredSessionAttemptWithSession stored;
        try {
            stored = sm
                .getSessionStore(siteId)
                // putAndLockSession + insertAttempt might be able to be faster by combining them into one method and optimize using a single SQL with CTE
                .putAndLockSession(session, (store, storedSession) -> {
                    StoredProject proj = rm.getProjectStore(siteId).getProjectById(projId);
                    if (proj.getDeletedAt().isPresent()) {
                        throw new ResourceNotFoundException(String.format(ENGLISH,
                                    "Project id={} name={} is already deleted",
                                    proj.getId(), proj.getName()));
                    }
                    StoredSessionAttempt storedAttempt = store.insertAttempt(storedSession.getId(), projId, attempt);  // this may throw ResourceConflictException

                    logger.info("Starting a new session project id={} workflow name={} session_time={}",
                            projId, ar.getWorkflowName(), SESSION_TIME_FORMATTER.withZone(ar.getTimeZone()).format(ar.getSessionTime()));

                    // root task is already ready to run
                    final Task rootTask = Task.taskBuilder()
                        .parentId(Optional.absent())
                        .fullName(root.getFullName())
                        .config(TaskConfig.validate(root.getConfig()))
                        .taskType(root.getTaskType())
                        .state(rootTaskState)
                        .stateFlags(TaskStateFlags.empty().withInitialTask())
                        .build();
                    store.insertRootTask(storedAttempt.getId(), rootTask, (taskStore, storedTaskId) -> {
                        TaskControl.addInitialTasksExceptingRootTask(taskStore, storedAttempt.getId(),
                                storedTaskId, tasks, resumingTasks);
                        return null;
                    });
                    if (!ar.getSessionMonitors().isEmpty()) {
                        for (SessionMonitor monitor : ar.getSessionMonitors()) {
                            logger.debug("Using session monitor: {}", monitor);
                        }
                        store.insertMonitors(storedAttempt.getId(), ar.getSessionMonitors());
                    }
                    return StoredSessionAttemptWithSession.of(siteId, storedSession, storedAttempt);
                });
        }
        catch (ResourceConflictException sessionAlreadyExists) {
            StoredSessionAttemptWithSession conflicted;
            if (ar.getRetryAttemptName().isPresent()) {
                conflicted = sm.getSessionStore(siteId)
                    .getAttemptByName(session.getProjectId(), session.getWorkflowName(), session.getSessionTime(), ar.getRetryAttemptName().get());
            }
            else {
                conflicted = sm.getSessionStore(siteId)
                    .getLastAttemptByName(session.getProjectId(), session.getWorkflowName(), session.getSessionTime());
            }
            throw new SessionAttemptConflictException("Session already exists", sessionAlreadyExists, conflicted);
        }

        if (rootTaskState == TaskStateCode.READY) {
            // this is an optimization to dispatch tasks to a queue quickly.
            try {
                enqueueTask(dispatcher, stored.getId());
            }
            catch (Exception ex) {
                // fallback to the normal operation.
                // exception of the optimization shouldn't be propagated to
                // the caller. enqueueReadyTasks will get the same error later.
                noticeStatusPropagate();
            }
        }
        else {
            noticeStatusPropagate();
        }

        return stored;
    }

    public boolean killAttemptById(int siteId, long attemptId)
        throws ResourceNotFoundException
    {
        StoredSessionAttemptWithSession attempt = sm.getSessionStore(siteId).getAttemptById(attemptId);
        boolean updated = sm.requestCancelAttempt(attempt.getId());

        if (updated) {
            noticeStatusPropagate();
        }

        return updated;
    }

    private void noticeStatusPropagate()
    {
        propagatorLock.lock();
        try {
            propagatorNotice = true;
            propagatorCondition.signalAll();
        }
        finally {
            propagatorLock.unlock();
        }
    }

    public void noticeRunWhileConditionChange()
    {
        propagatorLock.lock();
        try {
            // don't set propagatorNotice but break wait in runWhile
            propagatorCondition.signalAll();
        }
        finally {
            propagatorLock.unlock();
        }
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
        return sm.getAttemptWithSessionById(attemptId);
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
        try (TaskQueuer queuer = new TaskQueuer()) {
            Instant date = sm.getStoreTime();
            propagateAllBlockedToReady();
            retryRetryWaitingTasks();
            enqueueReadyTasks(queuer);  // TODO enqueue all (not only first 100)
            propagateAllPlannedToDone();
            propagateSessionArchive();

            //IncrementalStatusPropagator prop = new IncrementalStatusPropagator(date);  // TODO doesn't work yet
            int waitMsec = INITIAL_INTERVAL;
            while (cond.getAsBoolean()) {
                //boolean inced = prop.run();
                //boolean retried = retryRetryWaitingTasks();
                //if (inced || retried) {
                //    enqueueReadyTasks(queuer);
                //    propagatorNotice = true;
                //}

                propagateAllBlockedToReady();
                retryRetryWaitingTasks();
                enqueueReadyTasks(queuer);
                boolean someDone = propagateAllPlannedToDone();

                if (someDone) {
                    propagateSessionArchive();
                }
                else {
                    propagatorLock.lock();
                    try {
                        if (propagatorNotice) {
                            propagatorNotice = false;
                            waitMsec = INITIAL_INTERVAL;
                        }
                        else {
                            boolean noticed = propagatorCondition.await(waitMsec, TimeUnit.MILLISECONDS);
                            if (noticed && propagatorNotice) {
                                propagatorNotice = false;
                                waitMsec = INITIAL_INTERVAL;
                            }
                            else {
                                waitMsec = Math.min(waitMsec * 2, MAX_INTERVAL);
                            }
                        }
                    }
                    finally {
                        propagatorLock.unlock();
                    }
                }
            }
        }
    }

    private boolean propagateAllBlockedToReady()
    {
        boolean anyChanged = false;
        long lastTaskId = 0;
        Set<Long> checkedParentIds = new HashSet<>();
        while (true) {
            List<TaskStateSummary> tasks = sm.findTasksByState(TaskStateCode.BLOCKED, lastTaskId);
            if (tasks.isEmpty()) {
                break;
            }
            anyChanged =
                tasks
                .stream()
                .map(summary -> {
                    if (summary.getParentId().isPresent()) {
                        long parentId = summary.getParentId().get();
                        if (checkedParentIds.add(parentId)) {
                            return sm.lockTaskIfExists(parentId, (store) ->
                                store.trySetChildrenBlockedToReadyOrShortCircuitPlannedOrCanceled(parentId) > 0
                            ).or(false);
                        }
                        return false;
                    }
                    else {
                        // root task can't be BLOCKED. See submitWorkflow
                        return false;
                        //return sm.lockTaskIfExists(summary.getId(), (store) -> {
                        //    return store.setRootPlannedToReady(summary.getId());
                        //}).or(false);
                    }
                })
                .reduce(anyChanged, (a, b) -> a || b);
            lastTaskId = tasks.get(tasks.size() - 1).getId();
        }
        return anyChanged;
    }

    private boolean propagateAllPlannedToDone()
    {
        boolean anyChanged = false;
        long lastTaskId = 0;
        while (true) {
            List<TaskStateSummary> tasks = sm.findTasksByState(TaskStateCode.PLANNED, lastTaskId);
            if (tasks.isEmpty()) {
                break;
            }
            anyChanged =
                tasks
                .stream()
                .map(summary -> {
                    return sm.lockTaskIfExists(summary.getId(), (store, storedTask) ->
                        setDoneFromDoneChildren(new TaskControl(store, storedTask))
                    ).or(false);
                })
                .reduce(anyChanged, (a, b) -> a || b);
            lastTaskId = tasks.get(tasks.size() - 1).getId();
        }
        return anyChanged;
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
            RetryControl retryControl = RetryControl.prepare(task.getConfig().getMerged(), task.getStateParams(), false);  // don't retry by default

            boolean willRetry = retryControl.evaluate();
            List<Long> errorTaskIds;
            if (!willRetry) {
                errorTaskIds = addErrorTasksIfAny(lockedTask,
                        true,
                        (export) -> collectErrorParams(export, lockedTask.get()));
            }
            else {
                errorTaskIds = ImmutableList.of();
            }
            boolean updated;
            if (willRetry) {
                updated = lockedTask.setPlannedToGroupRetryWaiting(
                        retryControl.getNextRetryStateParams(),
                        retryControl.getNextRetryInterval());
            }
            else if (!errorTaskIds.isEmpty()) {
                // don't set GROUP_ERROR but set DELAYED_GROUP_ERROR. Delay until next setDoneFromDoneChildren call
                updated = lockedTask.setPlannedToPlannedWithDelayedGroupError();  // TODO set flag
            }
            else {
                updated = lockedTask.setPlannedToGroupError();
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

    private Config collectErrorParams(Config export, StoredTask task)
    {
        List<Long> childrenFromThis;
        {
            TaskTree tree = new TaskTree(sm.getTaskRelations(task.getAttemptId()));
            childrenFromThis = tree.getRecursiveChildrenIdList(task.getId());
        }

        Config error = cf.create();
        {
            List<Config> childrenErrors = sm.getErrors(childrenFromThis);
            for (Config childError : childrenErrors) {
                error.merge(childError);
            }
        }

        Config storeParams = cf.create();
        {
            List<Config> childrenStoreParams = sm.getStoreParams(childrenFromThis);
            for (Config childStoreParams : childrenStoreParams) {
                storeParams.merge(childStoreParams);
            }
        }

        return export
            .merge(storeParams)
            .set("error", error);
    }

    private boolean propagateSessionArchive()
    {
        boolean anyChanged = false;
        long lastTaskId = 0;
        while (true) {
            List<TaskAttemptSummary> tasks = sm.findRootTasksByStates(TaskStateCode.doneStates(), lastTaskId);
            if (tasks.isEmpty()) {
                break;
            }
            anyChanged =
                tasks
                .stream()
                .map(task -> {
                    return sm.lockAttemptIfExists(task.getAttemptId(), (store, summary) -> {
                        if (summary.getStateFlags().isDone()) {
                            // already archived. This means that another thread archived
                            // this attempt after findRootTasksByStates call.
                            return false;
                        }
                        else {
                            SessionAttemptControl control = new SessionAttemptControl(store, task.getAttemptId());
                            control.archiveTasks(archiveMapper, task.getState() == TaskStateCode.SUCCESS);
                            return true;
                        }
                    }).or(false);
                })
                .reduce(anyChanged, (a, b) -> a || b);
            lastTaskId = tasks.get(tasks.size() - 1).getId();
        }
        return anyChanged;
    }

    private class IncrementalStatusPropagator
    {
        private Instant updatedSince;

        private Instant lastUpdatedAt;
        private long lastUpdatedId;

        public IncrementalStatusPropagator(Instant updatedSince)
        {
            this.updatedSince = updatedSince;
        }

        public boolean run()
        {
            return propagateStatus();
        }

        private synchronized boolean propagateStatus()
        {
            boolean anyChanged = false;
            Set<Long> checkedParentIds = new HashSet<>();

            Instant nextUpdatedSince = sm.getStoreTime();
            lastUpdatedAt = updatedSince;
            lastUpdatedId = 0;

            while (true) {
                List<TaskStateSummary> tasks = sm.findRecentlyChangedTasks(lastUpdatedAt, lastUpdatedId);
                if (tasks.isEmpty()) {
                    break;
                }
                anyChanged =
                    tasks
                    .stream()
                    .map(task -> {
                        boolean propagatedToChildren = false;
                        boolean propagatedFromChildren = false;
                        boolean propagatedToSelf = false;

                        if (Tasks.isDone(task.getState()) || task.getState() == TaskStateCode.PLANNED) {
                            // this parent became planned or done. may be transite from planned to done immediately
                            propagatedToSelf = sm.lockTaskIfExists(task.getId(), (store, storedTask) -> {
                                return setDoneFromDoneChildren(new TaskControl(store, storedTask));
                            }).or(false);

                            if (!propagatedToSelf) {
                                // if this task is not done yet, transite children from blocked to ready
                                propagatedToChildren = sm.lockTaskIfExists(task.getId(), (store) ->
                                    // collect parameters and set them to ready tasks at the same time? no, because children's export_params/store_params are not propagated to parents
                                    store.trySetChildrenBlockedToReadyOrShortCircuitPlannedOrCanceled(task.getId()) > 0
                                ).or(false);
                            }
                        }

                        if (Tasks.isDone(task.getState())) {
                            if (task.getParentId().isPresent()) {
                                // this child became done. try to transite parent from planned to done.
                                // and dependint siblings tasks may be able to start
                                if (checkedParentIds.add(task.getParentId().get())) {
                                    propagatedFromChildren = sm.lockTaskIfExists(task.getParentId().get(), (store, storedTask) -> {
                                        boolean doneFromChildren = setDoneFromDoneChildren(new TaskControl(store, storedTask));
                                        boolean siblingsToReady = store.trySetChildrenBlockedToReadyOrShortCircuitPlannedOrCanceled(task.getId()) > 0;
                                        return doneFromChildren || siblingsToReady;
                                    }).or(false);
                                }
                            }
                            else {
                                // root task became done.
                                // TODO return archiveSession(task.getid());
                                //logger.info("Root task is done with state {}",
                                //        task.getState());
                            }
                        }

                        lastUpdatedAt = task.getUpdatedAt();
                        lastUpdatedId = task.getId();

                        return propagatedToChildren || propagatedFromChildren || propagatedToSelf;
                    })
                    .reduce(anyChanged, (a, b) -> a || b);
            }
            updatedSince = nextUpdatedSince;

            return anyChanged;
        }
    }

    private boolean retryRetryWaitingTasks()
    {
        return sm.trySetRetryWaitingToReady() > 0;
    }

    private class TaskQueuer
            implements AutoCloseable
    {
        private final Map<Long, Future<Void>> waiting = new ConcurrentHashMap<>();
        private final ExecutorService executor;

        public TaskQueuer()
        {
            this.executor = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("task-queuer-%d")
                    .build()
                    );
        }

        public void close()
        {
            // TODO
            executor.shutdown();
            //executor.awaitShutdown(10, TimeUnit.SECONDS);
            //executor.shutdownNow();
        }

        //public synchronized Future<Void> asyncEnqueueTask(final long taskId)
        //{
        //    if (waiting.containsKey(taskId)) {
        //        return waiting.get(taskId);
        //    }
        //    Future<Void> future = executor.submit(() -> {
        //        try {
        //            enqueueTask(dispatcher, taskId);
        //        }
        //        catch (Throwable t) {
        //            logger.error("Uncaught exception during enqueuing a task request. This enqueue attempt will be retried", t);
        //        }
        //        finally {
        //            waiting.remove(taskId);
        //        }
        //        return null;
        //    });
        //    waiting.put(taskId, future);
        //    return future;
        //}
    }

    private void enqueueReadyTasks(TaskQueuer queuer)
    {
        for (long taskId : sm.findAllReadyTaskIds(100)) {  // TODO randomize this resut to achieve concurrency
            enqueueTask(dispatcher, taskId);
            //queuer.asyncEnqueueTask(taskId);  // TODO async queuing is probably unnecessary but not sure
        }
    }

    private void enqueueTask(final TaskQueueDispatcher dispatcher, final long taskId)
    {
        sm.lockTaskIfExists(taskId, (store, task) -> {
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
                logger.error("Enqueue error, making this task failed: {}", task, ex);
                // TODO retry here?
                return taskFailed(lockedTask,
                        buildExceptionErrorConfig(ex).toConfig(cf));
            }
        }).or(false);
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

    private static long parseTaskIdFromEncodedQueuedTaskName(String encodedUniqueQueuedTaskName)
    {
        int posDot = encodedUniqueQueuedTaskName.indexOf('.');
        if (posDot >= 0) {
            return Long.parseLong(encodedUniqueQueuedTaskName.substring(0, posDot));
        }
        else {
            return Long.parseLong(encodedUniqueQueuedTaskName);
        }
    }

    // called by InProcessTaskServerApi
    public List<TaskRequest> getTaskRequests(List<TaskQueueLock> locks)
    {
        ImmutableList.Builder<TaskRequest> builder = ImmutableList.builder();
        for (TaskQueueLock lock : locks) {
            try {
                long taskId = parseTaskIdFromEncodedQueuedTaskName(lock.getUniqueName());
                Optional<TaskRequest> request = getTaskRequest(taskId, lock.getLockId());
                if (request.isPresent()) {
                    builder.add(request.get());
                }
                else {
                    dispatcher.deleteInconsistentTask(lock.getLockId());
                }
            }
            catch (RuntimeException ex) {
                logger.error("Invalid association of task queue lock id: {}", lock, ex);
            }
        }
        return builder.build();
    }

    private Optional<TaskRequest> getTaskRequest(long taskId, String lockId)
    {
        return sm.<Optional<TaskRequest>>lockTaskIfExists(taskId, (store, task) -> {
            StoredSessionAttemptWithSession attempt;
            try {
                attempt = sm.getAttemptWithSessionById(task.getAttemptId());
            }
            catch (ResourceNotFoundException ex) {
                Exception error = new IllegalStateException("Task id="+taskId+" is in the task queue but associated session attempt does not exist.", ex);
                logger.error("Database state error enqueuing task.", error);
                return Optional.absent();
            }

            Optional<StoredRevision> rev = Optional.absent();
            if (attempt.getWorkflowDefinitionId().isPresent()) {
                try {
                    rev = Optional.of(rm.getRevisionOfWorkflowDefinition(attempt.getWorkflowDefinitionId().get()));
                }
                catch (ResourceNotFoundException ex) {
                    Exception error = new IllegalStateException("Task id="+taskId+" is in the task queue but associated workflow definition does not exist.", ex);
                    logger.error("Database state error enqueuing task.", error);
                    return Optional.absent();
                }
            }

            StoredProject project;
            try {
                project = rm.getProjectByIdInternal(attempt.getSession().getProjectId());
            }
            catch (ResourceNotFoundException ex) {
                Exception error = new IllegalStateException("Task id=" + taskId + " is in the task queue but associated project does not exist.", ex);
                logger.error("Database state error enqueuing task.", error);
                return Optional.absent();
            }

            // merge order is:
            //   revision default < attempt < task < runtime
            Config params = cf.fromJsonString(systemConfig.get("digdag.defaultParams", String.class, "{}"));
            if (rev.isPresent()) {
                params.merge(rev.get().getDefaultParams());
            }
            params.merge(attempt.getParams());
            collectParams(params, task, attempt);

            // remove conditional subtasks that may cause JavaScript evaluation error if they include reference to a nested field such as
            // this_will_be_set_at_this_task.this_is_null.this_access_causes_error.
            // _do is another conditional subtsaks but they are kept remained here and removed later in ConfigEvalEngine because
            // operator factory needs _do while _check and _error are used only by WorkflowExecutor.
            Config localConfig = task.getConfig().getLocal().deepCopy();
            params.remove("_check");
            params.remove("_error");
            localConfig.remove("_check");
            localConfig.remove("_error");

            // TODO what should here do if the task is canceled? Add another flag field to TaskRequest
            //      so that Operator can handle it? Skipping task silently is probably not good idea
            //      because Operator may want to run cleanup process.

            // create TaskRequest for OperatorManager.
            // OperatorManager will ignore localConfig because it reloads config from dagfile_path with using the lates params.
            // TaskRequest.config usually stores params merged with local config. but here passes only params (local config is not merged)
            // so that OperatorManager can build it using the reloaded local config.
            TaskRequest request = TaskRequest.builder()
                .siteId(attempt.getSiteId())
                .projectId(attempt.getSession().getProjectId())
                .projectName(project.getName())
                .workflowName(attempt.getSession().getWorkflowName())
                .revision(rev.transform(it -> it.getName()))
                .taskId(task.getId())
                .attemptId(attempt.getId())
                .sessionId(attempt.getSessionId())
                .retryAttemptName(attempt.getRetryAttemptName())
                .taskName(task.getFullName())
                .lockId(lockId)
                .timeZone(attempt.getTimeZone())
                .sessionUuid(attempt.getSessionUuid())
                .sessionTime(attempt.getSession().getSessionTime())
                .createdAt(Instant.now())
                .localConfig(localConfig)
                .config(params)
                .lastStateParams(task.getStateParams())
                .build();

            return Optional.of(request);
        })
        .or(() -> {
            Exception error = new IllegalStateException("Task id="+taskId+" is in the task queue but associated task is deleted.");
            logger.error("Database state error enqueuing task.", error);
            return Optional.<TaskRequest>absent();
        });
    }

    private boolean retryGroupingTask(TaskControl lockedTask)
    {
        // rest task state of subtasks
        StoredTask task = lockedTask.get();

        TaskTree tree = new TaskTree(sm.getTaskRelations(task.getAttemptId()));
        List<Long> childrenIdList = tree.getRecursiveChildrenIdList(task.getId());
        lockedTask.copyInitialTasksForRetry(childrenIdList);

        lockedTask.setGroupRetryReadyToPlanned();

        return true;
    }

    public boolean taskFailed(int siteId, long taskId, String lockId, AgentId agentId,
            Config error)
    {
        boolean changed = sm.lockTaskIfExists(taskId, (store, task) ->
            taskFailed(new TaskControl(store, task), error)
        ).or(false);
        if (changed) {
            try {
                dispatcher.taskFinished(siteId, lockId, agentId);
            }
            catch (TaskNotFoundException ex) {
                logger.warn("Ignoring missing task entry error", ex);
            }
            catch (TaskConflictException ex) {
                logger.warn("Ignoring preempted task entry error", ex);
            }
        }
        return changed;
    }

    public boolean taskSucceeded(int siteId, long taskId, String lockId, AgentId agentId,
            TaskResult result)
    {
        boolean changed = sm.lockTaskIfExists(taskId, (store, task) ->
            taskSucceeded(new TaskControl(store, task),
                    result)
        ).or(false);
        if (changed) {
            try {
                dispatcher.taskFinished(siteId, lockId, agentId);
            }
            catch (TaskNotFoundException ex) {
                logger.warn("Ignoring missing task entry error", ex);
            }
            catch (TaskConflictException ex) {
                logger.warn("Ignoring preempted task entry error", ex);
            }
        }
        return changed;
    }

    public boolean retryTask(int siteId, long taskId, String lockId, AgentId agentId,
            int retryInterval, Config retryStateParams,
            Optional<Config> error)
    {
        boolean changed = sm.lockTaskIfExists(taskId, (store, task) ->
            retryTask(new TaskControl(store, task),
                retryInterval, retryStateParams,
                error)
        ).or(false);
        if (changed) {
            try {
                dispatcher.taskFinished(siteId, lockId, agentId);
            }
            catch (TaskNotFoundException ex) {
                logger.warn("Ignoring missing task entry error", ex);
            }
            catch (TaskConflictException ex) {
                logger.warn("Ignoring preempted task entry error", ex);
            }
        }
        return changed;
    }

    private boolean taskFailed(TaskControl lockedTask, Config error)
    {
        logger.trace("Task failed with error {} with no retry: {}",
                error, lockedTask.get());

        if (lockedTask.getState() != TaskStateCode.RUNNING) {
            logger.trace("Skipping taskFailed callback to a {} task",
                    lockedTask.getState());
            return false;
        }

        // task failed. add ^error tasks
        List<Long> errorTaskIds = addErrorTasksIfAny(lockedTask, false, (export) -> export.set("error", error));
        boolean updated;
        if (!errorTaskIds.isEmpty()) {
            logger.trace("Added an error task");
            // transition from planned to error is delayed until setDoneFromDoneChildren
            updated = lockedTask.setRunningToPlannedWithDelayedError(error);
        }
        else {
            updated = lockedTask.setRunningToShortCircuitError(error);
        }

        noticeStatusPropagate();

        if (!updated) {
            // return value of setRunningToRetryWaiting, setRunningToPlannedSuccessful, or setRunningToShortCircuitError
            // must be true because this task is locked
            // (won't be updated by other machines concurrently) and confirmed that
            // current state is RUNNING.
            logger.warn("Unexpected state change failure from RUNNING to RETRY, PLANNED or ERROR: {}", lockedTask.get());
        }
        return updated;
    }

    private boolean taskSucceeded(TaskControl lockedTask,
            TaskResult result)
    {
        logger.trace("Task succeeded with result {}: {}",
                result, lockedTask.get());

        if (lockedTask.getState() != TaskStateCode.RUNNING) {
            logger.debug("Ignoring taskSucceeded callback to a {} task",
                    lockedTask.getState());
            return false;
        }

        // task successfully finished. add :sub and :check tasks
        Optional<Long> rootSubtaskId = addSubtasksIfNotEmpty(lockedTask, result.getSubtaskConfig());
        addCheckTasksIfAny(lockedTask, rootSubtaskId);
        boolean updated = lockedTask.setRunningToPlannedSuccessful(result);

        noticeStatusPropagate();

        if (!updated) {
            // return value of setRunningToPlannedSuccessful must be true because this task is locked
            // (won't be updated by other machines concurrently) and confirmed that
            // current state is RUNNING.
            logger.warn("Unexpected state change failure from RUNNING to PLANNED: {}", lockedTask.get());
        }
        return updated;
    }

    private boolean retryTask(TaskControl lockedTask,
            int retryInterval, Config retryStateParams,
            Optional<Config> error)
    {
        if (lockedTask.getState() != TaskStateCode.RUNNING) {
            logger.trace("Skipping retryTask callback to a {} task",
                    lockedTask.getState());
            return false;
        }

        if (error.isPresent()) {
            logger.trace("Task failed with error {} with retrying after {} seconds: {}",
                    error.get(), retryInterval, lockedTask.get());
        }

        boolean updated = lockedTask.setRunningToRetryWaiting(retryStateParams, retryInterval);

        noticeStatusPropagate();

        if (!updated) {
            // return value of setRunningToRetryWaiting must be true because this task is locked
            // (won't be updated by other machines concurrently) and confirmed that
            // current state is RUNNING.
            logger.warn("Unexpected state change failure from RUNNING to RETRY: {}", lockedTask.get());
        }
        return updated;
    }

    private void collectParams(Config params, StoredTask task, StoredSessionAttempt attempt)
    {
        List<Long> parentsFromRoot;
        List<Long> parentsUpstreamChildrenFromFar;
        {
            TaskTree tree = new TaskTree(sm.getTaskRelations(attempt.getId()));
            parentsFromRoot = tree.getRecursiveParentIdListFromRoot(task.getId());
            parentsUpstreamChildrenFromFar = tree.getRecursiveParentsUpstreamChildrenIdListFromFar(task.getId());
        }

        // task merge order is:
        //   export < store < local
        List<Config> exports = sm.getExportParams(parentsFromRoot);
        List<Config> stores = sm.getStoreParams(parentsUpstreamChildrenFromFar);
        for (int si=0; si < parentsUpstreamChildrenFromFar.size(); si++) {
            Config stored = stores.get(si);
            long taskId = parentsUpstreamChildrenFromFar.get(si);
            int ei = parentsFromRoot.indexOf(taskId);
            if (ei >= 0) {
                // this is a parent task of the task
                Config exported = exports.get(ei);
                params.merge(exported);
            }
            params.merge(stored);
        }
        params.merge(task.getConfig().getExport());
    }

    private Optional<Long> addSubtasksIfNotEmpty(TaskControl lockedTask, Config subtaskConfig)
    {
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        WorkflowTaskList tasks = compiler.compileTasks(lockedTask.get().getFullName(), "^sub", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding sub tasks: {}", tasks);
        long rootTaskId = lockedTask.addGeneratedSubtasks(tasks, ImmutableList.of(), true);
        return Optional.of(rootTaskId);
    }

    private List<Long> addErrorTasksIfAny(TaskControl lockedTask, boolean isParentErrorPropagatedFromChildren, Function<Config, Config> errorBuilder)
    {
        List<Long> taskIds = new ArrayList<>();

        boolean isRootTask = lockedTask.get().getParentId().isPresent();
        if (!isRootTask) {
            taskIds.add(addAttemptFailureAlertTask(lockedTask));
        }

        Config subtaskConfig = lockedTask.get().getConfig().getErrorConfig();
        if (subtaskConfig.isEmpty()) {
            return taskIds;
        }

        // modify export params
        Config export = subtaskConfig.getNestedOrGetEmpty("_export");
        export = errorBuilder.apply(export);
        subtaskConfig.setNested("_export", export);

        WorkflowTaskList tasks = compiler.compileTasks(lockedTask.get().getFullName(), "^error", subtaskConfig);
        if (tasks.isEmpty()) {
            return taskIds;
        }

        logger.trace("Adding error tasks: {}", tasks);
        long rootTaskId = lockedTask.addGeneratedSubtasks(tasks, ImmutableList.of(), false);
        taskIds.add(rootTaskId);
        return taskIds;
    }

    private long addAttemptFailureAlertTask(TaskControl rootTask)
    {
        Config config = cf.create();
        config.set("_type", "notify");
        config.set("_command", "Workflow session attempt failed");
        WorkflowTaskList tasks = compiler.compileTasks(rootTask.get().getFullName(), "^failure-alert", config);
        return rootTask.addGeneratedSubtasks(tasks, ImmutableList.of(), false);
    }

    private Optional<Long> addCheckTasksIfAny(TaskControl lockedTask, Optional<Long> upstreamTaskId)
    {
        Config subtaskConfig = lockedTask.get().getConfig().getCheckConfig();
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        WorkflowTaskList tasks = compiler.compileTasks(lockedTask.get().getFullName(), "^check", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding check tasks: {}"+tasks);
        List<Long> upstreamTaskIdList = upstreamTaskId.transform(id -> ImmutableList.of(id)).or(ImmutableList.of());
        long rootTaskId = lockedTask.addGeneratedSubtasks(tasks, upstreamTaskIdList, false);
        return Optional.of(rootTaskId);
    }

    public Optional<Long> addMonitorTask(TaskControl lockedTask, String type, Config taskConfig)
    {
        switch (type) {
            case "sla":
                taskConfig.remove("time");
                taskConfig.remove("duration");
                addSlaMonitorTasks(lockedTask, type, taskConfig);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported monitor task type: " + type);
        }

        WorkflowTaskList tasks = compiler.compileTasks(lockedTask.get().getFullName(), "^" + type, taskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding {} tasks: {}", type, tasks);
        long rootTaskId = lockedTask.addGeneratedSubtasks(tasks, ImmutableList.of(), false);
        return Optional.of(rootTaskId);
    }

    private void addSlaMonitorTasks(TaskControl lockedTask, String type, Config taskConfig)
    {
        // TODO: validate sla configuration parameters ahead of time instead of potentially failing at runtime

        // Send an alert?
        boolean alert = true;
        try {
            alert = taskConfig.get("alert", boolean.class, true);
        }
        catch (ConfigException e) {
            logger.warn("sla configuration error: ", e);
        }
        taskConfig.remove("alert");
        if (alert) {
            Config config = cf.create();
            config.set("_type", "notify");
            config.set("_command", "SLA violation");
            WorkflowTaskList tasks = compiler.compileTasks(lockedTask.get().getFullName(), "^" + type + "^alert", config);
            logger.trace("Adding {} tasks: {}", type, tasks);
            // TODO: attempt should not fail if the alert notification task fails
            lockedTask.addGeneratedSubtasks(tasks, ImmutableList.of(), false);
        }

        // Fail the attempt?
        boolean fail = false;
        try {
            fail = taskConfig.get("fail", boolean.class, false);
        }
        catch (ConfigException e) {
            logger.warn("sla configuration error: ", e);
        }
        taskConfig.remove("fail");
        if (fail) {
            Config config = cf.create();
            config.set("_type", "fail");
            config.set("_command", "SLA violation");
            WorkflowTaskList tasks = compiler.compileTasks(lockedTask.get().getFullName(), "^" + type + "^fail", config);
            logger.trace("Adding {} tasks: {}", type, tasks);
            lockedTask.addGeneratedSubtasks(tasks, ImmutableList.of(), false);
        }
    }
}
