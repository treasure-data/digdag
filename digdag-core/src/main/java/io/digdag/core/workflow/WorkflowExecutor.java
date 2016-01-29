package io.digdag.core.workflow;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.time.ZoneId;
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
import io.digdag.core.agent.TaskRunnerManager;
import io.digdag.core.session.*;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskReport;
import io.digdag.spi.TaskInfo;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.RepositoryStoreManager;
import io.digdag.core.repository.StoredRepository;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.workflow.TaskMatchPattern.MultipleTaskMatchException;
import io.digdag.core.workflow.TaskMatchPattern.NoMatchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;

public class WorkflowExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutor.class);

    private final RepositoryStoreManager rm;
    private final SessionStoreManager sm;
    private final WorkflowCompiler compiler;
    private final ConfigFactory cf;
    private final ObjectMapper archiveMapper;

    private final Lock propagatorLock = new ReentrantLock();
    private final Condition propagatorCondition = propagatorLock.newCondition();
    private volatile boolean propagatorNotice = false;

    @Inject
    public WorkflowExecutor(
            RepositoryStoreManager rm,
            SessionStoreManager sm,
            WorkflowCompiler compiler,
            ConfigFactory cf,
            ObjectMapper archiveMapper)
    {
        this.rm = rm;
        this.sm = sm;
        this.compiler = compiler;
        this.cf = cf;
        this.archiveMapper = archiveMapper;
    }

    public StoredSessionAttemptWithSession submitSubworkflow(int siteId, AttemptRequest ar,
            WorkflowDefinition def, SubtaskMatchPattern subtaskMatchPattern,
            List<SessionMonitor> monitors)
        throws SessionAttemptConflictException, NoMatchException, MultipleTaskMatchException
    {
        Workflow workflow = compiler.compile(def.getName(), def.getConfig());
        WorkflowTaskList sourceTasks = workflow.getTasks();

        int fromIndex = subtaskMatchPattern.findIndex(sourceTasks);  // this may return 0
        WorkflowTaskList tasks = (fromIndex > 0) ?
            SubtaskExtract.extract(sourceTasks, fromIndex) :
            sourceTasks;

        logger.debug("Checking a session of workflow '{}' ({}) from task {} with overwrite parameters: {}",
                def.getName(),
                def.getConfig().getNestedOrGetEmpty("meta"),
                fromIndex,
                ar.getOverwriteParams());

        return submitTasks(siteId, ar, tasks, monitors);
    }

    public StoredSessionAttemptWithSession submitWorkflow(int siteId,
            AttemptRequest ar,
            WorkflowDefinition def,
            List<SessionMonitor> monitors)
        throws SessionAttemptConflictException
    {
        Workflow workflow = compiler.compile(def.getName(), def.getConfig());
        WorkflowTaskList tasks = workflow.getTasks();

        logger.debug("Checking a session of workflow '{}' ({}) with overwrite parameters: {}",
                def.getName(),
                def.getConfig().getNestedOrGetEmpty("meta"),
                ar.getOverwriteParams());

        return submitTasks(siteId, ar, tasks, monitors);
    }

    public StoredSessionAttemptWithSession submitTasks(int siteId, AttemptRequest ar,
            WorkflowTaskList tasks, List<SessionMonitor> monitors)
        throws SessionAttemptConflictException
    {
        for (WorkflowTask task : tasks) {
            logger.trace("  Step["+task.getIndex()+"]: "+task.getName());
            logger.trace("    parent: "+task.getParentIndex().transform(it -> Integer.toString(it)).or("(root)"));
            logger.trace("    upstreams: "+task.getUpstreamIndexes().stream().map(it -> Integer.toString(it)).collect(Collectors.joining(", ")));
            logger.trace("    config: "+task.getConfig());
        }

        Session session = Session.of(ar.getRepositoryId(), ar.getWorkflowName(), ar.getInstant());

        // TODO get tiemzone from default params, overwrite params, or ar.getDefaultTimeZone
        // TODO set timezone, session_time, etc.
        Config sessionParams =
            ar.getRevisionDefaultParams().deepCopy()
            .setAll(ar.getOverwriteParams());

        SessionAttempt attempt = SessionAttempt.of(
                ar.getRetryAttemptName(),
                sessionParams, ar.getStoredWorkflowDefinitionId());

        TaskConfig.validateAttempt(attempt);

        StoredSessionAttempt stored;
        try {
            final WorkflowTask root = tasks.get(0);
            stored = sm
                .getSessionStore(siteId)
                .putAndLockSession(session, (store, storedSession) -> {
                    StoredSessionAttempt storedAttempt;
                    storedAttempt = store.insertAttempt(storedSession.getId(), ar.getRepositoryId(), attempt);  // this may throw ResourceConflictException:

                    logger.info("Starting a new session repository id={} workflow name={} instant={}",
                            ar.getRepositoryId(), ar.getWorkflowName(), ar.getInstant());

                    final Task rootTask = Task.taskBuilder()
                        .parentId(Optional.absent())
                        .fullName(root.getName())
                        .config(TaskConfig.validate(root.getConfig()))
                        .taskType(root.getTaskType())
                        .state(root.getTaskType().isGroupingOnly() ? TaskStateCode.PLANNED : TaskStateCode.READY)  // root task is already ready to run
                        .build();
                    store.insertRootTask(storedAttempt.getId(), rootTask, (taskStore, storedTask) -> {
                        new TaskControl(taskStore, storedTask).addTasksExceptingRootTask(tasks);
                        return null;
                    });
                    store.insertMonitors(storedAttempt.getId(), monitors);
                    return storedAttempt;
                });
        }
        catch (ResourceConflictException sessionAlreadyExists) {
            try {
                StoredSessionAttemptWithSession conflicted;
                if (ar.getRetryAttemptName().isPresent()) {
                    conflicted = sm.getSessionStore(siteId)
                        .getSessionAttemptByNames(session.getRepositoryId(), session.getWorkflowName(), session.getInstant(), ar.getRetryAttemptName().get());
                }
                else {
                    conflicted = sm.getSessionStore(siteId)
                        .getLastSessionAttemptByNames(session.getRepositoryId(), session.getWorkflowName(), session.getInstant());
                }
                throw new SessionAttemptConflictException("Session already exists", conflicted);
            }
            catch (ResourceNotFoundException shouldNotHappen) {
                throw new IllegalStateException("Database state error", shouldNotHappen);
            }
        }

        noticeStatusPropagate();  // TODO is this necessary?

        return StoredSessionAttemptWithSession.of(siteId, session, stored);
    }

    public boolean killSessionById(int siteId, long attemptId)
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

    public void run(TaskQueueDispatcher dispatcher)
            throws InterruptedException
    {
        runUntil(dispatcher, () -> true);
    }

    public void runUntilAny(TaskQueueDispatcher dispatcher)
            throws InterruptedException
    {
        runUntil(dispatcher, () -> sm.isAnyNotDoneSessions());
    }

    private static final int INITIAL_INTERVAL = 100;
    private static final int MAX_INTERVAL = 5000;

    private void runUntil(TaskQueueDispatcher dispatcher, BooleanSupplier cond)
            throws InterruptedException
    {
        try (TaskQueuer queuer = new TaskQueuer(dispatcher)) {
            Instant date = sm.getStoreTime();
            propagateAllBlockedToReady();
            retryRetryWaitingTasks();
            propagateAllPlannedToDone();
            propagateSessionArchive();
            enqueueReadyTasks(queuer);  // TODO enqueue all (not only first 100)

            IncrementalStatusPropagator prop = new IncrementalStatusPropagator(date);  // TODO doesn't work yet
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
                propagateAllPlannedToDone();
                propagateSessionArchive();
                enqueueReadyTasks(queuer);

                propagatorLock.lock();
                try {
                    if (propagatorNotice) {
                        propagatorNotice = false;
                        waitMsec = INITIAL_INTERVAL;
                    }
                    else {
                        propagatorCondition.await(waitMsec, TimeUnit.MILLISECONDS);
                        waitMsec = Math.min(waitMsec * 2, MAX_INTERVAL);
                    }
                }
                finally {
                    propagatorLock.unlock();
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

        List<Config> childrenErrors = lockedTask.collectChildrenErrors();
        if (childrenErrors.isEmpty() && !task.getError().isPresent()) {
            boolean updated = lockedTask.setPlannedToSuccess();

            if (!updated) {
                // return value of setPlannedToSuccess must be true because this tack is locked
                // (won't be updated by other machines concurrently) and confirmed that
                // current state is PLANNED.
                logger.warn("Unexpected state change failure from PLANNED to SUCCESS: {}", task);
            }
            return updated;
        }
        else if (task.getError().isPresent()) {
            boolean updated;
            if (task.getStateParams().has("group_error")) {
                // this error was formerly delayed by last setDoneFromDoneChildren call
                // TODO group_error is unnecessary if error (task.getError()) has such information
                updated = lockedTask.setPlannedToGroupError(task.getStateParams(), task.getError().get());
            }
            else {
                // this error was formerly delayed by taskFailed
                updated = lockedTask.setPlannedToError(task.getStateParams(), task.getError().get());
            }

            if (!updated) {
                // return value of setPlannedToGroupError or setPlannedToError must be true because this tack is locked
                // (won't be updated by other machines concurrently) and confirmed that
                // current state is PLANNED.
                logger.warn("Unexpected state change failure from PLANNED to ERROR or GROUP_ERROR: {}", task);
            }
            return updated;
        }
        else {
            // group error
            Config error = buildPropagatedError(childrenErrors);
            RetryControl retryControl = RetryControl.prepare(task.getConfig(), task.getStateParams(), false);  // don't retry by default

            boolean willRetry = retryControl.evaluate(error);
            Optional<StoredTask> errorTask = addErrorTasksIfAny(lockedTask, error,
                    willRetry ? Optional.of(retryControl.getNextRetryInterval()) : Optional.absent(),
                    true);
            boolean updated;
            if (willRetry) {
                int retryInterval = retryControl.getNextRetryInterval();
                updated = lockedTask.setPlannedToGroupRetry(
                        retryControl.getNextRetryStateParams(),
                        retryControl.getNextRetryInterval());
            }
            else if (errorTask.isPresent()) {
                // don't set GROUP_ERROR here. Delay until next setDoneFromDoneChildren call
                Config nextState = task.getStateParams()
                    .deepCopy()
                    .set("group_error", true);
                updated = lockedTask.setPlannedToPlanned(nextState, error);
            }
            else {
                updated = lockedTask.setPlannedToGroupError(task.getStateParams(), error);
            }

            if (!updated) {
                // return value of setPlannedToGroupError, setPlannedToPlanned, or setPlannedToGroupError
                // must be true because this tack is locked
                // (won't be updated by other machines concurrently) and confirmed that
                // current state is PLANNED.
                logger.warn("Unexpected state change failure from PLANNED to PLANNED GROUP_RETRY, or GROUP_ERROR: {}", task);
            }
            return updated;
        }
    }

    private Config buildPropagatedError(List<Config> childrenErrors)
    {
        Preconditions.checkState(!childrenErrors.isEmpty(), "errors must not be empty to migrate to children_error state");
        return childrenErrors.get(0).getFactory().create().set("errors", childrenErrors);
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
                                    // collect parameters and set them to ready tasks at the same time? no, because children's carry_params are not propagated to parents
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
        private final TaskQueueDispatcher dispatcher;
        private final Map<Long, Future<Void>> waiting = new ConcurrentHashMap<>();
        private final ExecutorService executor;

        public TaskQueuer(TaskQueueDispatcher dispatcher)
        {
            this.dispatcher = dispatcher;
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
                    logger.error("Uncaught exception", t);
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
            queuer.asyncEnqueueTask(taskId);
        }
    }

    private void enqueueTask(final TaskQueueDispatcher dispatcher, final long taskId)
    {
        sm.lockTaskIfExists(taskId, (store, task) -> {
            TaskControl lockedTask = new TaskControl(store, task);
            if (lockedTask.getState() != TaskStateCode.READY) {
                return false;
            }

            String fullName = task.getFullName();
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
                //   revision default < attempt < task
                Config params;
                if (rev.isPresent()) {
                    params = rev.get().getDefaultParams().deepCopy()
                        .setAll(attempt.getParams());
                }
                else {
                    params = attempt.getParams().deepCopy();
                }

                collectParams(params, task, attempt);
                TaskConfig.setRuntimeBuiltInParams(params, attempt);

                // create TaskRequest for TaskRunnerManager.
                // TaskRunnerManager will ignore localConfig because it reloads config from dagfile_path with using the lates params.
                // TaskRequest.config usually stores params merged with local config. but here passes only params (local config is not merged)
                // so that TaskRunnerManager can build it using the reloaded local config.
                TaskRequest request = TaskRequest.builder()
                    .taskInfo(
                            TaskInfo.of(
                                task.getId(),
                                attempt.getSiteId(),
                                attempt.getId(),
                                attempt.getRetryAttemptName(),
                                fullName))
                    .repositoryId(attempt.getSession().getRepositoryId())
                    .workflowName(attempt.getSession().getWorkflowName())
                    .revision(rev.transform(it -> it.getName()))
                    .dagfilePath(rev.transform(it -> it.getDagfilePath()))
                    .localConfig(task.getConfig().getLocal())
                    .config(params)
                    .lastStateParams(task.getStateParams())
                    .build();

                if (task.getStateFlags().isCancelRequested()) {
                    return lockedTask.setToCanceled();
                }

                logger.debug("Queuing task: "+request);
                dispatcher.dispatch(request);

                boolean updated = lockedTask.setReadyToRunning();
                if (!updated) {
                    // return value of setReadyToRunning must be true because this tack is locked
                    // (won't be updated by other machines concurrently) and confirmed that
                    // current state is READY.
                    logger.warn("Unexpected state change failure from READY to RUNNING: {}", task);
                }

                return updated;
            }
            catch (Exception ex) {
                Config stateParams = cf.create().set("schedule_error", ex.toString());
                return taskFailed(lockedTask,
                        TaskRunnerManager.makeExceptionError(cf, ex), stateParams,
                        Optional.absent());  // TODO retry here?
            }
        }).or(false);
    }

    public boolean taskFailed(long taskId,
            final Config error, final Config stateParams,
            final Optional<Integer> retryInterval)
    {
        return sm.lockTaskIfExists(taskId, (store, task) ->
            taskFailed(new TaskControl(store, task),
                    error, stateParams,
                    retryInterval)
        ).or(false);
    }

    public boolean taskSucceeded(long taskId,
            final Config stateParams, final Config subtaskConfig,
            final TaskReport report)
    {
        return sm.lockTaskIfExists(taskId, (store, task) ->
            taskSucceeded(new TaskControl(store, task),
                    stateParams, subtaskConfig,
                    report)
        ).or(false);
    }

    public boolean taskPollNext(long taskId,
            final Config stateParams, final int retryInterval)
    {
        return sm.lockTaskIfExists(taskId, (store, task) ->
            taskPollNext(new TaskControl(store, task),
                    stateParams, retryInterval)
        ).or(false);
    }

    private boolean taskFailed(TaskControl lockedTask,
            Config error, Config stateParams,
            Optional<Integer> retryInterval)
    {
        StoredTask task = lockedTask.get();
        logger.trace("Task failed with error {} with {}: {}",
                error, retryInterval.transform(it -> "retrying after "+it+" seconds").or("no retry"), task);

        if (task.getState() != TaskStateCode.RUNNING) {
            logger.trace("Skipping taskFailed callback to a {} task",
                    task.getState());
            return false;
        }

        // task failed. add :error tasks
        Optional<StoredTask> errorTask = addErrorTasksIfAny(lockedTask, error, retryInterval, false);
        boolean updated;
        if (retryInterval.isPresent()) {
            logger.trace("Retrying the failed task");
            updated = lockedTask.setRunningToRetry(stateParams, error, retryInterval.get());
        }
        else if (errorTask.isPresent()) {
            logger.trace("Added an error task");
            // transition to error is delayed until setDoneFromDoneChildren
            updated = lockedTask.setRunningToPlanned(stateParams, error);
        }
        else {
            updated = lockedTask.setRunningToShortCircuitError(stateParams, error);
        }

        noticeStatusPropagate();

        if (!updated) {
            // return value of setRunningToRetry, setRunningToPlanned, or setRunningToShortCircuitError
            // must be true because this tack is locked
            // (won't be updated by other machines concurrently) and confirmed that
            // current state is RUNNING.
            logger.warn("Unexpected state change failure from RUNNING to RETRY, PLANNED or ERROR: {}", task);
        }
        return updated;
    }

    private boolean taskSucceeded(TaskControl lockedTask,
            Config stateParams, Config subtaskConfig,
            TaskReport report)
    {
        logger.trace("Task succeeded with report {}: {}",
                report, lockedTask.get());

        if (lockedTask.getState() != TaskStateCode.RUNNING) {
            logger.debug("Ignoring taskSucceeded callback to a {} task",
                    lockedTask.getState());
            return false;
        }

        // task successfully finished. add :sub and :check tasks
        Optional<StoredTask> subtaskRoot = addSubtasksIfNotEmpty(lockedTask, subtaskConfig);
        addCheckTasksIfAny(lockedTask, subtaskRoot);
        boolean updated = lockedTask.setRunningToPlanned(stateParams, report);

        noticeStatusPropagate();

        if (!updated) {
            // return value of setRunningToPlanned must be true because this tack is locked
            // (won't be updated by other machines concurrently) and confirmed that
            // current state is RUNNING.
            logger.warn("Unexpected state change failure from RUNNING to PLANNED: {}", lockedTask.get());
        }
        return updated;
    }

    private boolean taskPollNext(TaskControl lockedTask,
            Config stateParams, int retryInterval)
    {
        if (lockedTask.getState() != TaskStateCode.RUNNING) {
            logger.trace("Skipping taskPollNext callback to a {} task",
                    lockedTask.getState());
            return false;
        }

        boolean updated = lockedTask.setRunningToRetry(stateParams, retryInterval);

        noticeStatusPropagate();

        if (!updated) {
            // return value of setRunningToRetry must be true because this tack is locked
            // (won't be updated by other machines concurrently) and confirmed that
            // current state is RUNNING.
            logger.warn("Unexpected state change failure from RUNNING to RETRY: {}", lockedTask.get());
        }
        return updated;
    }

    private Config collectParams(Config params, StoredTask task, StoredSessionAttempt attempt)
    {
        List<Long> parentsFromRoot;
        List<Long> upstreamsFromFar;
        {
            TaskTree tree = new TaskTree(sm.getTaskRelations(attempt.getId()));
            parentsFromRoot = Lists.reverse(tree.getParentIdList(task.getId()));
            upstreamsFromFar = Lists.reverse(tree.getRecursiveParentsUpstreamChildrenIdList(task.getId()));
        }

        // task merge order is:
        //   export < carry from parents < carry from upstreams < local
        sm.getExportParams(parentsFromRoot)
            .stream().forEach(node -> params.setAll(node));
        params.setAll(task.getConfig().getExport());
        sm.getCarryParams(parentsFromRoot)
            .stream().forEach(node -> params.setAll(node));
        sm.getCarryParams(upstreamsFromFar)
            .stream().forEach(node -> params.setAll(node));

        return params;
    }

    private Optional<StoredTask> addSubtasksIfNotEmpty(TaskControl lockedTask, Config subtaskConfig)
    {
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        WorkflowTaskList tasks = compiler.compileTasks(":sub", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding sub tasks: {}"+tasks);
        StoredTask task = lockedTask.addSubtasks(tasks, ImmutableList.of(), true);
        return Optional.of(task);
    }

    private Optional<StoredTask> addErrorTasksIfAny(TaskControl lockedTask, Config error, Optional<Integer> parentRetryInterval, boolean isParentErrorPropagatedFromChildren)
    {
        Config subtaskConfig = lockedTask.get().getConfig().getErrorConfig();
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        // modify export params
        Config export = subtaskConfig.getNestedOrSetEmpty("export");
        export.set("error", error);

        WorkflowTaskList tasks = compiler.compileTasks(":error", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding error tasks: {}", tasks);
        StoredTask added = lockedTask.addSubtasks(tasks, ImmutableList.of(), false);
        return Optional.of(added);
    }

    private Optional<StoredTask> addCheckTasksIfAny(TaskControl lockedTask, Optional<StoredTask> upstreamTask)
    {
        Config subtaskConfig = lockedTask.get().getConfig().getCheckConfig();
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        WorkflowTaskList tasks = compiler.compileTasks(":check", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding check tasks: {}"+tasks);
        StoredTask added = lockedTask.addSubtasks(tasks, ImmutableList.of(), false);
        return Optional.of(added);
    }

    public Optional<StoredTask> addMonitorTask(TaskControl lockedTask, String type, Config taskConfig)
    {
        WorkflowTaskList tasks = compiler.compileTasks(":" + type, taskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding {} tasks: {}", type, tasks);
        StoredTask added = lockedTask.addSubtasks(tasks, ImmutableList.of(), false);
        return Optional.of(added);
    }
}
