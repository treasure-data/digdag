package io.digdag.core.workflow;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
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
import io.digdag.core.agent.RetryControl;
import io.digdag.core.agent.TaskRunner;
import io.digdag.core.queue.Action;
import io.digdag.core.spi.TaskReport;
import io.digdag.core.repository.WorkflowSource;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.config.Config;
import io.digdag.core.config.ConfigFactory;

public class WorkflowExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutor.class);

    private final SessionStoreManager sm;
    private final SessionMonitorManager monitorManager;
    private final WorkflowCompiler compiler;
    private final ConfigFactory cf;

    private final Lock propagatorLock = new ReentrantLock();
    private final Condition propagatorCondition = propagatorLock.newCondition();
    private volatile boolean propagatorNotice = false;

    @Inject
    public WorkflowExecutor(
            SessionStoreManager sm,
            WorkflowCompiler compiler,
            SessionMonitorManager monitorManager,
            ConfigFactory cf)
    {
        this.sm = sm;
        this.compiler = compiler;
        this.monitorManager = monitorManager;
        this.cf = cf;
    }

    public StoredSession submitWorkflow(
            WorkflowSource workflowSource, Session newSession, SessionRelation relation,
            Date slaCurrentTime, Optional<TaskMatchPattern> from)
        throws ResourceConflictException, TaskMatchPattern.MultipleMatchException, TaskMatchPattern.NoMatchException
    {
        Workflow workflow = compiler.compile(workflowSource.getName(), workflowSource.getConfig());
        WorkflowTaskList sourceTasks = workflow.getTasks();

        int fromIndex = 0;
        if (from.isPresent()) {
            fromIndex = from.get().findIndex(sourceTasks);
        }

        WorkflowTaskList tasks = (fromIndex > 0) ?
            SubtaskExtract.extract(sourceTasks, fromIndex) :
            sourceTasks;

        List<SessionMonitor> monitors = monitorManager.getMonitors(workflowSource, slaCurrentTime);

        logger.info("Starting a new session of workflow '{}' ({}) from index {} with session parameters: {}",
                workflowSource.getName(),
                workflowSource.getConfig().getNestedOrGetEmpty("meta"),
                fromIndex,
                newSession.getParams());
        for (WorkflowTask task : tasks) {
            logger.trace("  Step["+task.getIndex()+"]: "+task.getName());
            logger.trace("    parent: "+task.getParentIndex().transform(it -> Integer.toString(it)).or("(root)"));
            logger.trace("    upstreams: "+task.getUpstreamIndexes().stream().map(it -> Integer.toString(it)).collect(Collectors.joining(", ")));
            logger.trace("    config: "+task.getConfig());
        }

        final WorkflowTask root = tasks.get(0);
        return sm.newSession(newSession, relation, (StoredSession session, SessionStoreManager.SessionBuilderStore store) -> {
            final Task rootTask = Task.taskBuilder()
                .sessionId(session.getId())
                .parentId(Optional.absent())
                .fullName(root.getName())
                .config(root.getConfig())
                .taskType(root.getTaskType())
                .state(root.getTaskType().isGroupingOnly() ? TaskStateCode.PLANNED : TaskStateCode.READY)  // root task is already ready to run
                .build();
            store.addRootTask(rootTask, (control, lockedRootTask) -> {
                control.addTasksExceptingRootTask(lockedRootTask, tasks);
                return null;
            });
            store.addMonitors(session.getId(), monitors);
        });
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
        runUntil(dispatcher, () -> sm.isAnyNotDoneWorkflows());
    }

    private static final int INITIAL_INTERVAL = 100;
    private static final int MAX_INTERVAL = 5000;

    private void runUntil(TaskQueueDispatcher dispatcher, BooleanSupplier cond)
            throws InterruptedException
    {
        try (TaskQueuer queuer = new TaskQueuer(dispatcher)) {
            Date date = sm.getStoreTime();
            propagateAllBlockedToReady();
            propagateAllPlannedToDone();
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
                propagateAllPlannedToDone();
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
                .map(task -> {
                    if (task.getParentId().isPresent()) {
                        long parentId = task.getParentId().get();
                        if (checkedParentIds.add(parentId)) {
                            return sm.lockTaskIfExists(parentId, (TaskControl lockedParent) -> {
                                return lockedParent.trySetChildrenBlockedToReadyOrShortCircuitPlanned() > 0;
                            }).or(false);
                        }
                        return false;
                    }
                    else {
                        // root task can't be BLOCKED. See submitWorkflow
                        return false;
                        //return sm.lockTaskIfExists(task.getId(), (TaskControl lockedRoot) -> {
                        //    return lockedRoot.setRootPlannedToReady();
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
                .map(task -> {
                    return sm.lockTaskIfExists(task.getId(), (TaskControl lockedTask, StoredTask detail) -> {
                        return setDoneFromDoneChildren(lockedTask, detail);
                    }).or(false);
                })
                .reduce(anyChanged, (a, b) -> a || b);
            lastTaskId = tasks.get(tasks.size() - 1).getId();
        }
        return anyChanged;
    }

    private boolean setDoneFromDoneChildren(TaskControl lockedTask, StoredTask detail)
    {
        if (lockedTask.getState() != TaskStateCode.PLANNED) {
            return false;
        }
        if (!lockedTask.isAllChildrenDone()) {
            return false;
        }

        List<Config> childrenErrors = lockedTask.collectChildrenErrors();
        if (childrenErrors.isEmpty() && !detail.getError().isPresent()) {
            return lockedTask.setPlannedToSuccess();
        }
        else if (detail.getError().isPresent()) {
            if (detail.getStateParams().has("group_error")) {
                // this error was formerly delayed by last setDoneFromDoneChildren call
                // TODO group_error is unnecessary if error (detail.getError()) has such information
                return lockedTask.setPlannedToGroupError(detail.getStateParams(), detail.getError().get());
            }
            else {
                // this error was formerly delayed by taskFailed
                return lockedTask.setPlannedToError(detail.getStateParams(), detail.getError().get());
            }
        }
        else {
            // group error
            Config error = buildPropagatedError(childrenErrors);
            RetryControl retryControl = RetryControl.prepare(detail.getConfig(), detail.getStateParams(), false);  // don't retry by default

            boolean willRetry = retryControl.evaluate(error);
            Optional<StoredTask> errorTask = addErrorTasksIfAny(lockedTask, detail, error,
                    willRetry ? Optional.of(retryControl.getNextRetryInterval()) : Optional.absent(),
                    true);
            if (willRetry) {
                int retryInterval = retryControl.getNextRetryInterval();
                return lockedTask.setPlannedToGroupRetry(
                        retryControl.getNextRetryStateParams(),
                        retryControl.getNextRetryInterval());
            }
            else if (errorTask.isPresent()) {
                // don't set GROUP_ERROR here. Delay until next setDoneFromDoneChildren call
                Config nextState = detail.getStateParams()
                    .deepCopy()
                    .set("group_error", true);
                return lockedTask.setPlannedToPlanned(nextState, error);
            }
            else {
                return lockedTask.setPlannedToGroupError(detail.getStateParams(), error);
            }
        }
    }

    private Config buildPropagatedError(List<Config> childrenErrors)
    {
        Preconditions.checkState(!childrenErrors.isEmpty(), "errors must not be empty to migrate to children_error state");
        return childrenErrors.get(0).getFactory().create().set("errors", childrenErrors);
    }

    private class IncrementalStatusPropagator
    {
        private Date updatedSince;

        private Date lastUpdatedAt;
        private long lastUpdatedId;

        public IncrementalStatusPropagator(Date updatedSince)
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

            Date nextUpdatedSince = sm.getStoreTime();
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
                            propagatedToSelf = sm.lockTaskIfExists(task.getId(), (TaskControl lockedTask, StoredTask detail) -> {
                                return setDoneFromDoneChildren(lockedTask, detail);
                            }).or(false);

                            if (!propagatedToSelf) {
                                // if this task is not done yet, transite children from blocked to ready
                                propagatedToChildren = sm.lockTaskIfExists(task.getId(), (TaskControl lockedTask) -> {
                                    return lockedTask.trySetChildrenBlockedToReadyOrShortCircuitPlanned() > 0;
                                }).or(false);
                            }
                        }

                        if (Tasks.isDone(task.getState())) {
                            if (task.getParentId().isPresent()) {
                                // this child became done. try to transite parent from planned to done.
                                // and dependint siblings tasks may be able to start
                                if (checkedParentIds.add(task.getParentId().get())) {
                                    propagatedFromChildren = sm.lockTaskIfExists(task.getParentId().get(), (TaskControl lockedParent, StoredTask detail) -> {
                                        boolean doneFromChildren = setDoneFromDoneChildren(lockedParent, detail);
                                        boolean siblingsToReady = lockedParent.trySetChildrenBlockedToReadyOrShortCircuitPlanned() > 0;
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
        sm.lockTaskIfExists(taskId, (TaskControl control, StoredTask task) -> {
            if (control.getState() != TaskStateCode.READY) {
                return false;
            }

            String fullName = task.getFullName();
            StoredSession session;
            try {
                session = sm.getSessionById(task.getSessionId());
            }
            catch (ResourceNotFoundException ex) {
                Exception error = new IllegalStateException("Task id="+taskId+" is ready to run but associated session does not exist.", ex);
                logger.error("Database state error enquing task.", error);
                return false;
            }

            Optional<TaskReport> skipTaskReport = Optional.fromNullable(session.getOptions().getSkipTaskMap().get(fullName));
            if (skipTaskReport.isPresent()) {
                logger.debug("Skipping task '{}'", fullName);
                taskSucceeded(control, task,
                        cf.create(), cf.create(),
                        skipTaskReport.get());
                return true;
            }
            else {
                try {
                    Config params = collectTaskParams(task, session);
                    Config config = task.getConfig();  // TODO render using liquid?
                        Action action = Action.actionBuilder()
                        .taskId(task.getId())
                        .siteId(task.getSiteId())
                        .fullName(fullName)
                        .config(config)
                        .params(params)
                        .stateParams(task.getStateParams())
                        .build();

                    logger.debug("Queuing task: "+action);
                    dispatcher.dispatch(action);

                    control.setReadyToRunning();

                    return true;
                }
                catch (Exception ex) {
                    Config stateParams = cf.create().set("schedule_error", ex.toString());
                    taskFailed(control, task,
                            TaskRunner.makeExceptionError(cf, ex), stateParams,
                            Optional.absent());  // TODO retry here?
                }
            }

            return false;
        }).or(false);
    }

    public void taskFailed(long taskId,
            final Config error, final Config stateParams,
            final Optional<Integer> retryInterval)
    {
        sm.lockTaskIfExists(taskId, (TaskControl control, StoredTask task) -> {
            taskFailed(control, task,
                    error, stateParams,
                    retryInterval);
            return true;
        });
    }

    public void taskSucceeded(long taskId,
            final Config stateParams, final Config subtaskConfig,
            final TaskReport report)
    {
        sm.lockTaskIfExists(taskId, (TaskControl control, StoredTask task) -> {
            taskSucceeded(control, task,
                    stateParams, subtaskConfig,
                    report);
            return true;
        });
    }

    public void taskPollNext(long taskId,
            final Config stateParams, final int retryInterval)
    {
        sm.lockTaskIfExists(taskId, (TaskControl control, StoredTask task) -> {
            taskPollNext(control, task,
                    stateParams, retryInterval);
            return true;
        });
    }

    private void taskFailed(TaskControl control, StoredTask task,
            Config error, Config stateParams,
            Optional<Integer> retryInterval)
    {
        logger.trace("Task failed with error {} with {}: {}",
                error, retryInterval.transform(it -> "retrying after "+it+" seconds").or("no retry"), task);

        // task failed. add .error tasks
        Optional<StoredTask> errorTask = addErrorTasksIfAny(control, task, error, retryInterval, false);
        if (retryInterval.isPresent()) {
            control.setRunningToRetry(stateParams, error, retryInterval.get());
        }
        else if (errorTask.isPresent()) {
            // transition to error is delayed until setDoneFromDoneChildren
            control.setRunningToPlanned(stateParams, error);
        }
        else {
            control.setRunningToShortCircuitError(stateParams, error);
        }

        noticeStatusPropagate();
    }

    private void taskSucceeded(TaskControl control, StoredTask task,
            Config stateParams, Config subtaskConfig,
            TaskReport report)
    {
        logger.trace("Task succeeded with report {}: {}",
                report, task);

        // task successfully finished. add .sub and .check tasks
        Optional<StoredTask> subtaskRoot = addSubtasksIfNotEmpty(control, task, subtaskConfig);
        addCheckTasksIfAny(control, task, subtaskRoot);
        control.setRunningToPlanned(stateParams, report);

        noticeStatusPropagate();
    }

    private void taskPollNext(TaskControl control, StoredTask task,
            Config stateParams, int retryInterval)
    {
        control.setRunningToRetry(stateParams, retryInterval);

        noticeStatusPropagate();
    }

    private Config collectTaskParams(StoredTask task, StoredSession session)
    {
        // TODO not accurate implementation
        Config params = session.getParams().getFactory().create();
        params.set("session_name", session.getName());
        params.set("task_name", task.getFullName());
        params.setAll(session.getParams());
        params.setAll(task.getConfig().getNestedOrGetEmpty("params"));
        return collectTaskParams(task, params);
    }

    private Config collectTaskParams(StoredTask task, Config result)
    {
        // TODO not accurate implementation
        Optional<Long> lastId = Optional.absent();
        while (true) {
            List<StoredTask> ses = sm.getSessionStore(task.getSiteId()).getTasks(task.getSessionId(), 1024, lastId);
            if (ses.isEmpty()) {
                break;
            }
            for (StoredTask se : ses) {
                if (se.getReport().isPresent()) {
                    result.setAll(se.getReport().get().getCarryParams());
                }
                lastId = Optional.of(se.getId());
            }
        }
        return result;
    }

    private Optional<StoredTask> addSubtasksIfNotEmpty(TaskControl control, StoredTask detail, Config subtaskConfig)
    {
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        WorkflowTaskList tasks = compiler.compileTasks(".sub", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding sub tasks: {}"+tasks);
        StoredTask task = control.addSubtasks(detail, tasks, ImmutableList.of(), true);
        return Optional.of(task);
    }

    private Optional<StoredTask> addErrorTasksIfAny(TaskControl control, StoredTask detail, Config error, Optional<Integer> parentRetryInterval, boolean isParentErrorPropagatedFromChildren)
    {
        Config subtaskConfig = detail.getConfig().getNestedOrderedOrGetEmpty("error").deepCopy();
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        // modify params
        Config params = subtaskConfig.getNestedOrSetEmpty("params");
        params.set("error", error);

        WorkflowTaskList tasks = compiler.compileTasks(".error", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding error tasks: {}"+tasks);
        StoredTask task = control.addSubtasks(detail, tasks, ImmutableList.of(), false);
        return Optional.of(task);
    }

    private Optional<StoredTask> addCheckTasksIfAny(TaskControl control, StoredTask detail, Optional<StoredTask> upstreamTask)
    {
        Config subtaskConfig = detail.getConfig().getNestedOrderedOrGetEmpty("check").deepCopy();
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        WorkflowTaskList tasks = compiler.compileTasks(".check", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding check tasks: {}"+tasks);
        StoredTask task = control.addSubtasks(detail, tasks, ImmutableList.of(), false);
        return Optional.of(task);
    }

    public Optional<StoredTask> addSlaTask(TaskControl control, StoredTask detail, Config slaConfig)
    {
        WorkflowTaskList tasks = compiler.compileTasks(".sla", slaConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding sla tasks: {}"+tasks);
        StoredTask task = control.addSubtasks(detail, tasks, ImmutableList.of(), false);
        return Optional.of(task);
    }
}
