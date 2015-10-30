package io.digdag.core;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.Map.Entry;
import java.util.function.BooleanSupplier;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionExecutor
        implements TaskApi
{
    private static final Logger logger = LoggerFactory.getLogger(SessionExecutor.class);

    private final SessionStoreManager sm;
    private final WorkflowCompiler compiler;
    private final ConfigSourceFactory cf;

    private final Lock propagatorLock = new ReentrantLock();
    private final Condition propagatorCondition = propagatorLock.newCondition();
    private volatile boolean propagatorNotice = false;

    @Inject
    public SessionExecutor(
            SessionStoreManager sm,
            WorkflowCompiler compiler,
            ConfigSourceFactory cf)
    {
        this.sm = sm;
        this.compiler = compiler;
        this.cf = cf;
    }

    public StoredSession submitWorkflow(int siteId, StoredWorkflowSource workflowSource, Session newSession, SessionNamespace namespace)
    {
        Workflow workflow = compiler.compile(workflowSource.getName(), workflowSource.getConfig());
        List<WorkflowTask> tasks = workflow.getTasks();

        logger.info("Starting a new session of workflow '{}' ({}) with session parameters: {}",
                workflowSource.getName(),
                workflowSource.getConfig().getNestedOrGetEmpty("meta"),
                newSession.getParams());
        for (WorkflowTask task : tasks) {
            logger.trace("  Step["+task.getIndex()+"]: "+task.getName());
            logger.trace("    parent: "+task.getParentIndex().transform(it -> Integer.toString(it)).or("(root)"));
            logger.trace("    upstreams: "+task.getUpstreamIndexes().stream().map(it -> Integer.toString(it)).collect(Collectors.joining(", ")));
            logger.trace("    config: "+task.getConfig());
        }

        final WorkflowTask root = tasks.get(0);
        final List<WorkflowTask> sub = tasks.subList(1, tasks.size());
        return sm.newSession(siteId, newSession, namespace, (StoredSession session, SessionStoreManager.SessionBuilderStore store) -> {
            final Task rootTask = Task.taskBuilder()
                .sessionId(session.getId())
                .parentId(Optional.absent())
                .fullName(root.getName())
                .config(root.getConfig())
                .taskType(root.getTaskType())
                .state(root.getTaskType().isGroupingOnly() ? TaskStateCode.PLANNED : TaskStateCode.READY)  // root task is already ready to run
                .build();
            store.addRootTask(rootTask, (control, lockedRootTask) -> {
                control.addSubtasks(lockedRootTask, sub, 0);
                return null;
            });
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

    private void runUntil(TaskQueueDispatcher dispatcher, BooleanSupplier cond)
            throws InterruptedException
    {
        try (TaskStarter starter = new TaskStarter(dispatcher)) {
            Date date = sm.getStoreTime();
            propagateAllBlockedToReady();
            propagateAllPlannedToDone();
            enqueueReadyTasks(starter);  // TODO enqueue all (not only first 100)

            IncrementalStatusPropagator prop = new IncrementalStatusPropagator(date);  // TODO doesn't work yet
            while (cond.getAsBoolean()) {
                //boolean inced = prop.run();
                //boolean retried = retryRetryWaitingTasks();
                //if (inced || retried) {
                //    enqueueReadyTasks(starter);
                //    propagatorNotice = true;
                //}
                propagateAllBlockedToReady();
                propagateAllPlannedToDone();
                enqueueReadyTasks(starter);

                propagatorLock.lock();
                try {
                    if (propagatorNotice) {
                        propagatorNotice = false;
                    }
                    else {
                        propagatorCondition.await(2, TimeUnit.SECONDS);  // TODO use exponential back-off
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
                            return sm.lockTask(parentId, (TaskControl lockedParent) -> {
                                return lockedParent.trySetChildrenBlockedToReadyOrShortCircuitPlanned() > 0;
                            }).or(false);
                        }
                        return false;
                    }
                    else {
                        // root task can't be BLOCKED. See submitWorkflow
                        return false;
                        //return sm.lockTask(task.getId(), (TaskControl lockedRoot) -> {
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
                    return sm.lockTask(task.getId(), (TaskControl lockedTask, StoredTask detail) -> {
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

        List<ConfigSource> childrenErrors = lockedTask.collectChildrenErrors();
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
            ConfigSource error = buildPropagatedError(childrenErrors);
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
                ConfigSource nextState = detail.getStateParams()
                    .set("group_error", true);
                return lockedTask.setPlannedToPlanned(nextState, error);
            }
            else {
                return lockedTask.setPlannedToGroupError(detail.getStateParams(), error);
            }
        }
    }

    private ConfigSource buildPropagatedError(List<ConfigSource> childrenErrors)
    {
        Preconditions.checkState(!childrenErrors.isEmpty(), "errors must not be empty to migrate to children_error state");
        return childrenErrors.get(0).newConfigSource().set("errors", childrenErrors);
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
                            propagatedToSelf = sm.lockTask(task.getId(), (TaskControl lockedTask, StoredTask detail) -> {
                                return setDoneFromDoneChildren(lockedTask, detail);
                            }).or(false);

                            if (!propagatedToSelf) {
                                // if this task is not done yet, transite children from blocked to ready
                                propagatedToChildren = sm.lockTask(task.getId(), (TaskControl lockedTask) -> {
                                    return lockedTask.trySetChildrenBlockedToReadyOrShortCircuitPlanned() > 0;
                                }).or(false);
                            }
                        }

                        if (Tasks.isDone(task.getState())) {
                            if (task.getParentId().isPresent()) {
                                // this child became done. try to transite parent from planned to done.
                                // and dependint siblings tasks may be able to start
                                if (checkedParentIds.add(task.getParentId().get())) {
                                    propagatedFromChildren = sm.lockTask(task.getParentId().get(), (TaskControl lockedParent, StoredTask detail) -> {
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

    private class TaskStarter
            implements AutoCloseable
    {
        private final TaskQueueDispatcher dispatcher;
        private final Map<Long, Future<Void>> waiting = new HashMap<>();
        private final ExecutorService executor;

        public TaskStarter(TaskQueueDispatcher dispatcher)
        {
            this.dispatcher = dispatcher;
            this.executor = Executors.newCachedThreadPool();  // TODO thread name
        }

        public void close()
        {
            // TODO
            executor.shutdown();
            //executor.awaitShutdown(10, TimeUnit.SECONDS);
            //executor.shutdownNow();
        }

        public synchronized Future<Void> add(long taskId)
        {
            if (waiting.containsKey(taskId)) {
                return waiting.get(taskId);
            }
            Future<Void> future = executor.submit(new EnqueueTask(taskId));
            waiting.put(taskId, future);
            return future;
        }

        private class EnqueueTask
                implements Callable<Void>
        {
            private final long taskId;

            public EnqueueTask(long taskId)
            {
                this.taskId = taskId;
            }

            public Void call() throws Exception
            {
                try {
                    enqueueTask(dispatcher, taskId);
                }
                catch (Throwable t) {
                    logger.error("Uncaught exception", t);
                }
                finally {
                    synchronized(this) {
                        waiting.remove(taskId);
                    }
                }
                return null;
            }
        }
    }

    private void enqueueReadyTasks(TaskStarter starter)
    {
        for (long taskId : sm.findAllReadyTaskIds(100)) {  // TODO randomize this resut to achieve concurrency
            starter.add(taskId);
        }
    }

    private void enqueueTask(final TaskQueueDispatcher dispatcher, final long taskId)
    {
        sm.lockTask(taskId, (TaskControl control, StoredTask task) -> {
            if (control.getState() != TaskStateCode.READY) {
                return false;
            }

            String fullName = task.getFullName();
            StoredSession session = sm.getSessionStore(task.getSiteId()).getSessionById(task.getSessionId());

            Optional<TaskReport> skipTaskReport = Optional.fromNullable(session.getOptions().getSkipTaskMap().get(fullName));
            if (skipTaskReport.isPresent()) {
                logger.debug("Skipping task '{}'", fullName);
                taskSucceeded(control, task,
                        cf.create(), cf.create(),
                        cf.create(), skipTaskReport.get());
                return true;
            }
            else {
                try {
                    ConfigSource params = collectTaskParams(task, session.getParams().deepCopy());
                    ConfigSource config = task.getConfig();  // TODO render using liquid?
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
                    return true;
                }
                catch (Exception ex) {
                    ConfigSource stateParams = cf.create().set("schedule_error", ex.toString());
                    taskFailed(control, task,
                            TaskRunner.makeExceptionError(cf, ex), stateParams,
                            Optional.absent());  // TODO retry here?
                }
            }

            return false;
        }).or(false);
    }

    @Override
    public void taskFailed(long taskId,
            final ConfigSource error, final ConfigSource stateParams,
            final Optional<Integer> retryInterval)
    {
        sm.lockTask(taskId, (TaskControl control, StoredTask task) -> {
            taskFailed(control, task,
                    error, stateParams,
                    retryInterval);
            return true;
        });
    }

    @Override
    public void taskSucceeded(long taskId,
            final ConfigSource stateParams, final ConfigSource subtaskConfig,
            final ConfigSource carryParams, final TaskReport report)
    {
        sm.lockTask(taskId, (TaskControl control, StoredTask task) -> {
            taskSucceeded(control, task,
                    stateParams, subtaskConfig,
                    carryParams, report);
            return true;
        });
    }

    @Override
    public void taskPollNext(long taskId,
            final ConfigSource stateParams, final int retryInterval)
    {
        sm.lockTask(taskId, (TaskControl control, StoredTask task) -> {
            taskPollNext(control, task,
                    stateParams, retryInterval);
            return true;
        });
    }

    private void taskFailed(TaskControl control, StoredTask task,
            ConfigSource error, ConfigSource stateParams,
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
            ConfigSource stateParams, ConfigSource subtaskConfig,
            ConfigSource carryParams, TaskReport report)
    {
        logger.trace("Task succeeded with carry parameters {}: {}",
                carryParams, task);

        // task successfully finished. add .sub and .check tasks
        Optional<StoredTask> subtaskRoot = addSubtasksIfNotEmpty(control, task, subtaskConfig);
        addCheckTasksIfAny(control, task, subtaskRoot);
        control.setRunningToPlanned(stateParams, carryParams, report);

        noticeStatusPropagate();
    }

    private void taskPollNext(TaskControl control, StoredTask task,
            ConfigSource stateParams, int retryInterval)
    {
        control.setRunningToRetry(stateParams, retryInterval);

        noticeStatusPropagate();
    }

    private ConfigSource collectTaskParams(StoredTask task, ConfigSource result)
    {
        // TODO not accurate implementation
        Optional<Long> lastId = Optional.absent();
        while (true) {
            List<StoredTask> ses = sm.getSessionStore(task.getSiteId()).getTasks(task.getSessionId(), 1024, lastId);
            if (ses.isEmpty()) {
                break;
            }
            for (StoredTask se : ses) {
                result.setAll(se.getCarryParams());
                lastId = Optional.of(se.getId());
            }
        }
        return result;
    }

    private Optional<StoredTask> addSubtasksIfNotEmpty(TaskControl control, StoredTask detail, ConfigSource subtaskConfig)
    {
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        List<WorkflowTask> tasks = compiler.compileTasks(".sub", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding sub tasks: {}"+tasks);
        StoredTask task = control.addSubtasks(detail, tasks, ImmutableList.of(), true, 1);
        return Optional.of(task);
    }

    private Optional<StoredTask> addErrorTasksIfAny(TaskControl control, StoredTask detail, ConfigSource parentError, Optional<Integer> parentRetryInterval, boolean isParentErrorPropagatedFromChildren)
    {
        ConfigSource subtaskConfig = detail.getConfig().getNestedOrGetEmpty("error");
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        List<WorkflowTask> tasks = compiler.compileTasks(".error", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding error tasks: {}"+tasks);
        StoredTask task = control.addSubtasks(detail, tasks, ImmutableList.of(), false, 1);
        return Optional.of(task);
    }

    private Optional<StoredTask> addCheckTasksIfAny(TaskControl control, StoredTask detail, Optional<StoredTask> upstreamTask)
    {
        ConfigSource subtaskConfig = detail.getConfig().getNestedOrGetEmpty("check");
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        List<WorkflowTask> tasks = compiler.compileTasks(".check", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding check tasks: {}"+tasks);
        StoredTask task = control.addSubtasks(detail, tasks, ImmutableList.of(), false, 1);
        return Optional.of(task);
    }
}
