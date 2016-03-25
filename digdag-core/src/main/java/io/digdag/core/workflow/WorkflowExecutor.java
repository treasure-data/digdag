package io.digdag.core.workflow;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.function.BooleanSupplier;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.agent.RetryControl;
import io.digdag.core.agent.OperatorManager;
import io.digdag.core.agent.AgentId;
import io.digdag.core.session.*;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.RepositoryStoreManager;
import io.digdag.core.repository.StoredRepository;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import static java.util.Locale.ENGLISH;
import static io.digdag.core.queue.QueueSettingStore.DEFAULT_QUEUE_NAME;

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
 *
 * RUNNING:
 *
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

    private final RepositoryStoreManager rm;
    private final SessionStoreManager sm;
    private final WorkflowCompiler compiler;
    private final TaskQueueDispatcher dispatcher;
    private final ConfigFactory cf;
    private final ObjectMapper archiveMapper;

    private final Lock propagatorLock = new ReentrantLock();
    private final Condition propagatorCondition = propagatorLock.newCondition();
    private volatile boolean propagatorNotice = false;

    @Inject
    public WorkflowExecutor(
            RepositoryStoreManager rm,
            SessionStoreManager sm,
            TaskQueueDispatcher dispatcher,
            WorkflowCompiler compiler,
            ConfigFactory cf,
            ObjectMapper archiveMapper)
    {
        this.rm = rm;
        this.sm = sm;
        this.compiler = compiler;
        this.dispatcher = dispatcher;
        this.cf = cf;
        this.archiveMapper = archiveMapper;
    }

    public StoredSessionAttemptWithSession submitWorkflow(int siteId,
            AttemptRequest ar,
            WorkflowDefinition def)
        throws ResourceNotFoundException, SessionAttemptConflictException
    {
        Workflow workflow = compiler.compile(def.getName(), def.getConfig());  // TODO cache (CachedWorkflowCompiler which takes def id as the cache key)
        WorkflowTaskList tasks = workflow.getTasks();

        logger.debug("Checking a session of workflow '{}' ({}) with session parameters: {}",
                def.getName(),
                def.getConfig().getNestedOrGetEmpty("meta"),
                ar.getSessionParams());

        return submitTasks(siteId, ar, tasks);
    }

    private static final DateTimeFormatter SESSION_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxx", ENGLISH);

    public StoredSessionAttemptWithSession submitTasks(int siteId, AttemptRequest ar,
            WorkflowTaskList tasks)
        throws ResourceNotFoundException, SessionAttemptConflictException
    {
        for (WorkflowTask task : tasks) {
            logger.trace("  Step[{}]: {}", task.getIndex(), task.getName());
            logger.trace("    parent: {}", task.getParentIndex().transform(it -> Integer.toString(it)).or("(root)"));
            logger.trace("    upstreams: {}", task.getUpstreamIndexes().stream().map(it -> Integer.toString(it)).collect(Collectors.joining(", ")));
            logger.trace("    config: {}", task.getConfig());
        }

        int repoId = ar.getStored().getRepositoryId();
        Session session = Session.of(repoId, ar.getWorkflowName(), ar.getSessionTime());

        SessionAttempt attempt = SessionAttempt.of(
                ar.getRetryAttemptName(),
                ar.getSessionParams(),
                ar.getTimeZone(),
                Optional.of(ar.getStored().getWorkflowDefinitionId()));

        TaskConfig.validateAttempt(attempt);

        StoredSessionAttemptWithSession stored;
        try {
            final WorkflowTask root = tasks.get(0);
            stored = sm
                .getSessionStore(siteId)
                // putAndLockSession + insertAttempt might be able to be faster by combining them into one method and optimize using a single SQL with CTE
                .putAndLockSession(session, (store, storedSession) -> {
                    StoredSessionAttempt storedAttempt;
                    storedAttempt = store.insertAttempt(storedSession.getId(), repoId, attempt);  // this may throw ResourceConflictException

                    logger.info("Starting a new session repository id={} workflow name={} session_time={}",
                            repoId, ar.getWorkflowName(), SESSION_TIME_FORMATTER.withZone(ar.getTimeZone()).format(ar.getSessionTime()));

                    final Task rootTask = Task.taskBuilder()
                        .parentId(Optional.absent())
                        .fullName(root.getFullName())
                        .config(TaskConfig.validate(root.getConfig()))
                        .taskType(root.getTaskType())
                        .state(root.getTaskType().isGroupingOnly() ? TaskStateCode.PLANNED : TaskStateCode.READY)  // root task is already ready to run
                        .build();
                    store.insertRootTask(storedAttempt.getId(), rootTask, (taskStore, storedTaskId) -> {
                        TaskControl.addTasksExceptingRootTask(taskStore, storedAttempt.getId(),
                                storedTaskId, tasks);
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
                    .getSessionAttemptByNames(session.getRepositoryId(), session.getWorkflowName(), session.getSessionTime(), ar.getRetryAttemptName().get());
            }
            else {
                conflicted = sm.getSessionStore(siteId)
                    .getLastSessionAttemptByNames(session.getRepositoryId(), session.getWorkflowName(), session.getSessionTime());
            }
            throw new SessionAttemptConflictException("Session already exists", sessionAlreadyExists, conflicted);
        }

        try {
            // this is an optimization to dispatch tasks to a queue quickly.
            enqueueTask(dispatcher, stored.getId());
        }
        catch (Exception ex) {
            // fakkback to the normal operation.
            // exception of the optimization shouldn't be propagated to
            // the caller. asyncEnqueueTask will get the same error later.
            noticeStatusPropagate();
        }

        return stored;
    }

    public boolean killAttemptById(int siteId, long attemptId)
        throws ResourceNotFoundException
    {
        StoredSessionAttemptWithSession attempt = sm.getSessionStore(siteId).getSessionAttemptById(attemptId);
        boolean updated = sm.requestCancelAttempt(attempt.getId());

        if (updated) {
            noticeStatusPropagate();
        }

        // TODO sync kill requests to already-running tasks in queue

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

    private void runWhile(BooleanSupplier cond)
            throws InterruptedException
    {
        try (TaskQueuer queuer = new TaskQueuer()) {
            Instant date = sm.getStoreTime();
            propagateAllBlockedToReady();
            retryRetryWaitingTasks();
            propagateSessionArchive();
            enqueueReadyTasks(queuer);  // TODO enqueue all (not only first 100)
            propagateAllPlannedToDone();

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
                propagateSessionArchive();
                enqueueReadyTasks(queuer);
                boolean someDone = propagateAllPlannedToDone();

                if (!someDone) {
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
            RetryControl retryControl = RetryControl.prepare(task.getConfig(), task.getStateParams(), false);  // don't retry by default

            boolean willRetry = retryControl.evaluate();
            Optional<Long> errorTaskId;
            if (!willRetry) {
                errorTaskId = addErrorTasksIfAny(lockedTask,
                        true,
                        (export) -> collectErrorParams(export, lockedTask.get()));
            }
            else {
                errorTaskId = Optional.absent();
            }
            boolean updated;
            if (willRetry) {
                updated = lockedTask.setPlannedToGroupRetryWaiting(
                        retryControl.getNextRetryStateParams(),
                        retryControl.getNextRetryInterval());
            }
            else if (errorTaskId.isPresent()) {
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
                        SessionAttemptControl control = new SessionAttemptControl(store, task.getAttemptId());
                        control.archiveTasks(archiveMapper, task.getState() == TaskStateCode.SUCCESS);
                        return true;
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

        public synchronized Future<Void> asyncEnqueueTask(final long taskId)
        {
            if (waiting.containsKey(taskId)) {
                return waiting.get(taskId);
            }
            Future<Void> future = executor.submit(() -> {
                try {
                    enqueueTask(dispatcher, taskId);
                }
                catch (Throwable t) {
                    logger.error("Uncaught exception during enquing a task request. This enqueue attempt will be retried", t);
                }
                finally {
                    waiting.remove(taskId);
                }
                return null;
            });
            waiting.put(taskId, future);
            return future;
        }
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

            StoredSessionAttemptWithSession attempt;
            try {
                attempt = sm.getAttemptWithSessionById(task.getAttemptId());
            }
            catch (ResourceNotFoundException ex) {
                Exception error = new IllegalStateException("Task id="+taskId+" is ready to run but associated session attempt does not exist.", ex);
                logger.error("Database state error enquing task.", error);
                return false;
            }

            Optional<StoredRevision> rev = Optional.absent();
            if (attempt.getWorkflowDefinitionId().isPresent()) {
                try {
                    rev = Optional.of(rm.getRevisionOfWorkflowDefinition(attempt.getWorkflowDefinitionId().get()));
                }
                catch (ResourceNotFoundException ex) {
                    Exception error = new IllegalStateException("Task id="+taskId+" is ready to run but associated workflow definition does not exist.", ex);
                    logger.error("Database state error enquing task.", error);
                    return false;
                }
            }

            try {
                // merge order is:
                //   revision default < attempt < task < runtime
                Config params = attempt.getParams().getFactory().create();
                if (rev.isPresent()) {
                    params.merge(rev.get().getDefaultParams());
                }
                params.merge(attempt.getParams());
                collectParams(params, task, attempt);

                // create TaskRequest for OperatorManager.
                // OperatorManager will ignore localConfig because it reloads config from dagfile_path with using the lates params.
                // TaskRequest.config usually stores params merged with local config. but here passes only params (local config is not merged)
                // so that OperatorManager can build it using the reloaded local config.
                TaskRequest request = TaskRequest.builder()
                    .siteId(attempt.getSiteId())
                    .repositoryId(attempt.getSession().getRepositoryId())
                    .workflowName(attempt.getSession().getWorkflowName())
                    .revision(rev.transform(it -> it.getName()))
                    .taskId(task.getId())
                    .attemptId(attempt.getId())
                    .sessionId(attempt.getSessionId())
                    .retryAttemptName(attempt.getRetryAttemptName())
                    .taskName(task.getFullName())
                    .queueName(DEFAULT_QUEUE_NAME)  // TODO make this configurable
                    // TODO support queue resourceType
                    .lockId("")   // this will be overwritten by TaskQueueServer
                    .priority(0)  // TODO make this configurable
                    .timeZone(attempt.getTimeZone())
                    .sessionUuid(attempt.getSessionUuid())
                    .sessionTime(attempt.getSession().getSessionTime())
                    .createdAt(Instant.now())
                    .localConfig(task.getConfig().getLocal())
                    .config(params)
                    .lastStateParams(task.getStateParams())
                    .build();

                if (task.getStateFlags().isCancelRequested()) {
                    return lockedTask.setToCanceled();
                }

                logger.debug("Queuing task: "+request);
                dispatcher.dispatch(request);

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
                        OperatorManager.makeExceptionError(cf, ex));
            }
        }).or(false);
    }

    public boolean taskFailed(int siteId, long taskId, String lockId, AgentId agentId,
            Config error)
    {
        boolean changed = sm.lockTaskIfExists(taskId, (store, task) ->
            taskFailed(new TaskControl(store, task), error)
        ).or(false);
        if (changed) {
            dispatcher.taskFinished(siteId, lockId, agentId);
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
            dispatcher.taskFinished(siteId, lockId, agentId);
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
            dispatcher.taskFinished(siteId, lockId, agentId);
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
        Optional<Long> errorTaskId = addErrorTasksIfAny(lockedTask, false, (export) -> export.set("error", error));
        boolean updated;
        if (errorTaskId.isPresent()) {
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
            parentsFromRoot = Lists.reverse(tree.getRecursiveParentIdList(task.getId()));
            parentsUpstreamChildrenFromFar = Lists.reverse(tree.getRecursiveParentsUpstreamChildrenIdList(task.getId()));
        }

        // task merge order is:
        //   export < store < local
        List<Config> exports = sm.getExportParams(parentsFromRoot);
        List<Config> stores = sm.getStoreParams(parentsUpstreamChildrenFromFar);
        for (int si=0; si < parentsUpstreamChildrenFromFar.size(); si++) {
            Config s = stores.get(si);
            long taskId = parentsUpstreamChildrenFromFar.get(si);
            int ei = parentsFromRoot.indexOf(taskId);
            if (ei >= 0) {
                // this is a parent task of the task
                Config e = exports.get(ei);
                params.merge(e);
            }
            params.merge(s);
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

        logger.trace("Adding sub tasks: {}"+tasks);
        long rootTaskId = lockedTask.addSubtasks(tasks, ImmutableList.of(), true);
        return Optional.of(rootTaskId);
    }

    private Optional<Long> addErrorTasksIfAny(TaskControl lockedTask, boolean isParentErrorPropagatedFromChildren, Function<Config, Config> errorBuilder)
    {
        Config subtaskConfig = lockedTask.get().getConfig().getErrorConfig();
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        // modify export params
        Config export = subtaskConfig.getNestedOrGetEmpty("_export");
        export = errorBuilder.apply(export);
        subtaskConfig.setNested("_export", export);

        WorkflowTaskList tasks = compiler.compileTasks(lockedTask.get().getFullName(), "^error", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding error tasks: {}", tasks);
        long rootTaskId = lockedTask.addSubtasks(tasks, ImmutableList.of(), false);
        return Optional.of(rootTaskId);
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
        long rootTaskId = lockedTask.addSubtasks(tasks, upstreamTaskIdList, false);
        return Optional.of(rootTaskId);
    }

    public Optional<Long> addMonitorTask(TaskControl lockedTask, String type, Config taskConfig)
    {
        WorkflowTaskList tasks = compiler.compileTasks(lockedTask.get().getFullName(), "^" + type, taskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding {} tasks: {}", type, tasks);
        long rootTaskId = lockedTask.addSubtasks(tasks, ImmutableList.of(), false);
        return Optional.of(rootTaskId);
    }
}
