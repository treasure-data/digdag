package io.digdag.core;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
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

public class SessionExecutor
        implements TaskApi
{
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

    public StoredSession submitWorkflow(int siteId, StoredWorkflowSource workflowSource, Session newSession, SessionRelation relation)
    {
        Workflow workflow = compiler.compile(workflowSource.getName(), workflowSource.getConfig());
        List<WorkflowTask> tasks = workflow.getTasks();

        System.out.println("Running workflow "+workflow.getName()+" ("+workflow.getMeta()+")");
        //for (WorkflowTask task : tasks) {
        //    System.out.println("  Task["+task.getIndex()+"]: "+task.getName());
        //    System.out.println("    parent: "+task.getParentIndex().transform(it -> Integer.toString(it)).or(""));
        //    System.out.println("    upstreams: "+task.getUpstreamIndexes().stream().map(it -> Integer.toString(it)).collect(Collectors.joining(", ")));
        //    System.out.println("    config: "+task.getConfig());
        //}

        final WorkflowTask root = tasks.get(0);
        final List<WorkflowTask> sub = tasks.subList(1, tasks.size());
        return sm.newSession(siteId, newSession, relation, (StoredSession session, SessionStoreManager.SessionBuilderStore store) -> {
            final Task rootTask = Task.taskBuilder()
                .sessionId(session.getId())
                .parentId(Optional.absent())
                .fullName(root.getName())
                .config(root.getConfig())
                .taskType(root.getTaskType())
                .state(TaskStateCode.READY)
                .build();
            store.addRootTask(rootTask, (control, lockedRootTask) -> {
                control.addSubtasks(lockedRootTask, sub);
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

    public void showTasks()
    {
        for (StoredTask task : sm.getAllTasks()) {
            System.out.println("  Task["+task.getId()+"]: "+task.getFullName());
            System.out.println("    parent: "+task.getParentId().transform(it -> Long.toString(it)).or("(root)"));
            // TODO upstreams
            System.out.println("    state: "+task.getState());
            System.out.println("    retryAt: "+task.getRetryAt());
            System.out.println("    config: "+task.getConfig());
            System.out.println("    taskType: "+task.getTaskType());
            System.out.println("    stateParams: "+task.getStateParams());
            System.out.println("    carryParams: "+task.getCarryParams());
            System.out.println("    report: "+task.getReport());
            System.out.println("    error: "+task.getError());
        }
    }

    private void runUntil(TaskQueueDispatcher dispatcher, BooleanSupplier cond)
            throws InterruptedException
    {
        try (TaskStarter starter = new TaskStarter(dispatcher)) {
            Date date = sm.getStoreTime();
            propagateAllBlockedToReady();
            propagateAllPlannedToDone();
            enqueueReadyTasks(starter);  // TODO enqueue all (not only first 100)

            showTasks();

            IncrementalStatusPropagator prop = new IncrementalStatusPropagator(date);
            while (cond.getAsBoolean()) {
                System.out.println("running...");
                boolean inced = prop.run();
                boolean retried = retryRetryWaitingTasks();
                if (inced || retried) {
                    enqueueReadyTasks(starter);
                    propagatorNotice = true;
                }

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
                showTasks();
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
                        // root task
                        return sm.lockTask(task.getId(), (TaskControl lockedRoot) -> {
                            return lockedRoot.setRootPlannedToReady();
                        }).or(false);
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
            // this error was formerly delayed by taskFinished
            return lockedTask.setPlannedToError(detail.getStateParams(), detail.getError().get());
        }
        else {
            // group error
            // TODO add .error tasks and set error_task key to state params so that next time this doesn't add .error tasks?
            ConfigSource error = buildPropagatedError(childrenErrors);
            RetryControl retryControl = RetryControl.prepare(detail.getConfig(), detail.getStateParams(), false);  // don't retry by default
            if (retryControl.evaluate(error)) {
                int retryInterval = retryControl.getNextRetryInterval();
                return lockedTask.setPlannedToGroupRetry(
                        retryControl.getNextRetryStateParams(),
                        retryControl.getNextRetryInterval());
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
                                // this child became done. try to transite parent from planned to done
                                if (checkedParentIds.add(task.getParentId().get())) {
                                    propagatedFromChildren = sm.lockTask(task.getParentId().get(), (TaskControl lockedParent, StoredTask detail) -> {
                                        return setDoneFromDoneChildren(lockedParent, detail);
                                    }).or(false);
                                }
                            }
                            else {
                                // root task became done.
                                // TODO return archiveSession(task.getid());
                                System.out.println("root task is done: "+task);
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
                    // TODO
                    System.err.println("Uncaught exception: "+t);
                    t.printStackTrace(System.err);
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
                System.out.println("Skipping task: "+fullName);  // TODO logger
                taskSucceeded(control, task,
                        cf.create(), cf.create(),
                        cf.create(), skipTaskReport.get());
                return true;
            }
            else {
                try {
                    ConfigSource params = collectTaskParams(session.getParams().deepCopy());
                    ConfigSource config = task.getConfig();  // TODO render using liquid?
                        Action action = Action.actionBuilder()
                        .taskId(task.getId())
                        .siteId(task.getSiteId())
                        .fullName(fullName)
                        .config(config)
                        .params(params)
                        .stateParams(task.getStateParams())
                        .build();
                    System.out.println("dispatch action: "+action);
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
        System.out.println("task failed: "+error);
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
        // task successfully finished. add .sub and .check tasks
        Optional<StoredTask> subtaskRoot = addSubtasksIfAny(control, task, subtaskConfig);
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

    private ConfigSource collectTaskParams(ConfigSource result)
    {
        /// TODO not implemented yet
        return result;
    }

    private Optional<StoredTask> addErrorTasksIfAny(TaskControl control, StoredTask lockedParentTask, ConfigSource parentError, Optional<Integer> parentRetryInterval, boolean isParentErrorPropagatedFromChildren)
    {
        // TODO not implemented yet
        return Optional.absent();
    }

    private Optional<StoredTask> addSubtasksIfAny(TaskControl control, StoredTask lockedParentTask, ConfigSource subtaskConfig)
    {
        List<WorkflowTask> tasks = compiler.compileTasks(".sub", subtaskConfig);
        if (!tasks.isEmpty()) {
            final WorkflowTask root = tasks.get(0);
            final List<WorkflowTask> sub = tasks.subList(1, tasks.size());
            // TODO
            //return Optional.of(addTasks(control, lockedParentTask, subTasks, ImmutableList.of(), true));
            return Optional.absent();
        }
        else {
            return Optional.absent();
        }
    }

    private Optional<StoredTask> addCheckTasksIfAny(TaskControl control, StoredTask lockedParentTask, Optional<StoredTask> upstreamTask)
    {
        // TODO not implemented yet
        return Optional.absent();
    }

    //private StoredTask addTasks(TaskControl control, StoredTask lockedParentTask, List<WorkflowTask> tasks, List<Long> rootUpstreamIds, boolean cancelSiblings)
    //{
    //}
}
