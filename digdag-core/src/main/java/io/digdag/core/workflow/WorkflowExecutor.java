package io.digdag.core.workflow;

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
import io.digdag.metrics.DigdagTimed;
import io.digdag.spi.TaskQueueLock;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TaskQueueRequest;
import io.digdag.spi.TaskConflictException;
import io.digdag.spi.TaskNotFoundException;
import io.digdag.spi.metrics.DigdagMetrics;
import static io.digdag.spi.metrics.DigdagMetrics.Category;
import io.digdag.util.RetryControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;
import static java.util.Locale.ENGLISH;

/**
 * State transitions.
 *
 * BLOCKED:
 *   propagateBlockedChildrenToReady:
 *     store.trySetChildrenBlockedToReadyOrShortCircuitPlannedOrCanceled:
 *       (if GROUPING_ONLY flag is set) : PLANNED
 *       (if CANCEL_REQUESTED flag is set) : CANCELED
 *       : READY
 *   NOTE: propagateBlockedChildrenToReady is for non-root tasks. There
 *         are no methods that changes state of BLOCKED root tasks.
 *         Instead, WorkflowExecutor.submitTasks sets PLANNED or READY
 *         state when it inserts root tasks.
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
 *     (if subtasks or check task exist) lockedTask.setRunningToPlannedSuccessful:
 *       : PLANNED
 *     lockedTask.setRunningToShortCircuitSuccess:
 *       : SUCCESS
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
    private final TransactionManager tm;
    private final WorkflowCompiler compiler;
    private final TaskQueueDispatcher dispatcher;
    private final ConfigFactory cf;
    private final ObjectMapper archiveMapper;
    private final Config systemConfig;
    private final DigdagMetrics metrics;

    private final Lock propagatorLock = new ReentrantLock();
    private final Condition propagatorCondition = propagatorLock.newCondition();
    private volatile boolean propagatorNotice = false;

    @Inject
    public WorkflowExecutor(
            ProjectStoreManager rm,
            SessionStoreManager sm,
            TransactionManager tm,
            TaskQueueDispatcher dispatcher,
            WorkflowCompiler compiler,
            ConfigFactory cf,
            ObjectMapper archiveMapper,
            Config systemConfig,
            DigdagMetrics metrics)
    {
        this.rm = rm;
        this.sm = sm;
        this.tm = tm;
        this.compiler = compiler;
        this.dispatcher = dispatcher;
        this.cf = cf;
        this.archiveMapper = archiveMapper;
        this.systemConfig = systemConfig;
        this.metrics = metrics;
    }

    public Lock getPropagatorLock() { return propagatorLock; }
    public Condition getPropagatorCondition() { return propagatorCondition; }
    public boolean isPropagatorNotice() { return propagatorNotice; }
    public void setPropagatorNotice(boolean propagatorNotice) {
        this.propagatorNotice = propagatorNotice;
    }

    public StoredSessionAttemptWithSession submitWorkflow(int siteId,
            AttemptRequest ar,
            WorkflowDefinition def)
        throws ResourceNotFoundException, AttemptLimitExceededException, TaskLimitExceededException, SessionAttemptConflictException
    {
        Workflow workflow = compiler.compile(def.getName(), def.getConfig());  // TODO cache (CachedWorkflowCompiler which takes def id as the cache key)
        WorkflowTaskList tasks = workflow.getTasks();

        return submitTasks(siteId, ar, tasks);
    }

    private static final DateTimeFormatter SESSION_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxx", ENGLISH);

    public interface WorkflowSubmitterAction <T>
    {
        T call(WorkflowSubmitter submitter)
            throws ResourceNotFoundException, AttemptLimitExceededException, SessionAttemptConflictException;
    };

    public <T> T submitTransaction(int siteId, WorkflowSubmitterAction<T> func)
        throws ResourceNotFoundException, AttemptLimitExceededException, SessionAttemptConflictException
    {
        try {
            return sm.getSessionStore(siteId).sessionTransaction((transaction) -> {
                return func.call(new WorkflowSubmitter(siteId, transaction, rm.getProjectStore(siteId), sm.getSessionStore(siteId), tm));
            });
        }
        catch (Exception ex) {
            Throwables.propagateIfInstanceOf(ex, ResourceNotFoundException.class);
            Throwables.propagateIfInstanceOf(ex, AttemptLimitExceededException.class);
            Throwables.propagateIfInstanceOf(ex, SessionAttemptConflictException.class);
            throw Throwables.propagate(ex);
        }
    }

    public StoredSessionAttemptWithSession submitTasks(int siteId, AttemptRequest ar,
            WorkflowTaskList tasks)
        throws ResourceNotFoundException, AttemptLimitExceededException, TaskLimitExceededException, SessionAttemptConflictException
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

        List<ResumingTask> resumingTasks;
        if (ar.getResumingAttemptId().isPresent()) {
            WorkflowTask root = tasks.get(0);
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

        StoredSessionAttemptWithSession stored;
        try {
            SessionStore ss = sm.getSessionStore(siteId);

            long activeAttempts = ss.getActiveAttemptCount();
            if (activeAttempts + 1 > Limits.maxAttempts()) {
                throw new AttemptLimitExceededException("Too many attempts running. Limit: " + Limits.maxAttempts() + ", Current: " + activeAttempts);
            }

            stored = ss
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

                    StoredSessionAttemptWithSession storedAttemptWithSession =
                        StoredSessionAttemptWithSession.of(siteId, storedSession, storedAttempt);

                    try {
                        storeTasks(store, storedAttemptWithSession, tasks, resumingTasks, ar.getSessionMonitors());
                    }
                    catch (TaskLimitExceededException ex) {
                        throw new WorkflowTaskLimitExceededException(ex);
                    }

                    return storedAttemptWithSession;
                });
        }
        catch (WorkflowTaskLimitExceededException ex) {
            throw ex.getCause();
        }
        catch (ResourceConflictException sessionAlreadyExists) {
            tm.reset();
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

        noticeStatusPropagate();

        return stored;
    }

    public void storeTasks(
            SessionControlStore store,
            StoredSessionAttemptWithSession storedAttempt,
            WorkflowDefinition def,
            List<ResumingTask> resumingTasks,
            List<SessionMonitor> sessionMonitors)
        throws TaskLimitExceededException
    {
        Workflow workflow = compiler.compile(def.getName(), def.getConfig());
        WorkflowTaskList tasks = workflow.getTasks();

        storeTasks(store, storedAttempt, tasks, resumingTasks, sessionMonitors);
    }

    public void storeTasks(
            SessionControlStore store,
            StoredSessionAttemptWithSession storedAttempt,
            WorkflowTaskList tasks,
            List<ResumingTask> resumingTasks,
            List<SessionMonitor> sessionMonitors)
        throws TaskLimitExceededException
    {
        final WorkflowTask root = tasks.get(0);

        TaskStateCode rootTaskState = root.getTaskType().isGroupingOnly()
                ? TaskStateCode.PLANNED
                : TaskStateCode.READY;

        // root task is already ready to run
        final Task rootTask = Task.taskBuilder()
            .parentId(Optional.absent())
            .fullName(root.getFullName())
            .config(TaskConfig.validate(root.getConfig()))
            .taskType(root.getTaskType())
            .state(rootTaskState)
            .stateFlags(TaskStateFlags.empty().withInitialTask())
            .build();

        try {
            store.insertRootTask(storedAttempt.getId(), rootTask, (taskStore, storedTaskId) -> {
                try {
                    TaskControl.addInitialTasksExceptingRootTask(taskStore, storedAttempt.getId(),
                            storedTaskId, tasks, resumingTasks);
                }
                catch (TaskLimitExceededException ex) {
                    throw new WorkflowTaskLimitExceededException(ex);
                }
                return null;
            });
        }
        catch (WorkflowTaskLimitExceededException ex) {
            throw ex.getCause();
        }

        if (!sessionMonitors.isEmpty()) {
            for (SessionMonitor monitor : sessionMonitors) {
                logger.debug("Using session monitor: {}", monitor);
            }
            store.insertMonitors(storedAttempt.getId(), sessionMonitors);
        }
    }

    private static class WorkflowTaskLimitExceededException
            extends RuntimeException
    {
        public WorkflowTaskLimitExceededException(TaskLimitExceededException cause)
        {
            super(cause);
        }

        @Override
        public TaskLimitExceededException getCause()
        {
            return (TaskLimitExceededException) super.getCause();
        }
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

    /**
     * If catch exception then return defaultValue
     * @param func
     * @param defaultValue
     * @param errorMessage
     * @param <R>
     * @return
     */
    public <R> R catching(Supplier<R> func, R defaultValue, String errorMessage)
    {
        try {
            return func.get();
        }
        catch (Exception e) {
            catchingNotify(e);
            metrics.increment(Category.EXECUTOR, "errorInRunWhile");
            logger.warn(errorMessage);
            return defaultValue;
        }
    }

    /**
     * Called when catching() catches Exception.
     * Do noting.
     * Purpose for test and future extension.
     * @param e
     */
    public void catchingNotify(Exception e) {}

    public static class TaskQueuer
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
                tm.reset();
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
                tm.reset();
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
                    tm.reset();
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
                tm.reset();
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
                .workflowDefinitionId(attempt.getWorkflowDefinitionId())
                .build();

            return Optional.of(request);
        })
        .or(() -> {
            Exception error = new IllegalStateException("Task id="+taskId+" is in the task queue but associated task is deleted.");
            logger.error("Database state error enqueuing task.", error);
            return Optional.<TaskRequest>absent();
        });
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
                tm.reset();
                logger.warn("Ignoring missing task entry error", ex);
            }
            catch (TaskConflictException ex) {
                tm.reset();
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
                tm.reset();
                logger.warn("Ignoring missing task entry error", ex);
            }
            catch (TaskConflictException ex) {
                tm.reset();
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
                tm.reset();
                logger.warn("Ignoring missing task entry error", ex);
            }
            catch (TaskConflictException ex) {
                tm.reset();
                logger.warn("Ignoring preempted task entry error", ex);
            }
        }
        return changed;
    }

    public boolean taskFailed(TaskControl lockedTask, Config error)
    {
        logger.trace("Task failed with error {} with no retry: {}",
                error, lockedTask.get());

        if (lockedTask.getState() != TaskStateCode.RUNNING) {
            logger.trace("Skipping taskFailed callback to a {} task",
                    lockedTask.getState());
            return false;
        }

        if (lockedTask.get().getStateFlags().isCancelRequested()) {
            return lockedTask.setToCanceled();
        }

        // task failed. add ^error tasks
        boolean errorTaskAdded;
        try {
            Optional<Long> errorTaskId = addErrorTasksIfAny(lockedTask, false, (export) -> export.set("error", error));
            errorTaskAdded = errorTaskId.isPresent();
        }
        catch (TaskLimitExceededException ex) {
            tm.reset();
            errorTaskAdded = false;
            logger.warn("Failed to add error tasks because of task limit");
        }
        catch (ConfigException ex) {
            errorTaskAdded = false;
            logger.warn("Found a broken _error task in attempt {} task {}. Skipping this task.",
                    lockedTask.get().getAttemptId(), lockedTask.get().getId(), ex);
        }

        boolean updated;
        if (errorTaskAdded) {
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

        if (lockedTask.get().getStateFlags().isCancelRequested()) {
            return lockedTask.setToCanceled();
        }

        // task successfully finished. add ^sub and ^check tasks
        boolean subtaskAdded = false;
        try {
            Optional<Long> rootSubtaskId = addSubtasksIfNotEmpty(lockedTask, result.getSubtaskConfig());
            Optional<Long> checkTaskId = addCheckTasksIfAny(lockedTask, rootSubtaskId);
            subtaskAdded = rootSubtaskId.isPresent() || checkTaskId.isPresent();
        }
        catch (TaskLimitExceededException ex) {
            tm.reset();
            logger.warn("Failed to add subtasks because of task limit");
            return taskFailed(lockedTask, buildExceptionErrorConfig(ex).toConfig(cf));
        }
        catch (ConfigException ex) {
            // Subtask config or _check task config has a problem (e.g. more than one operator).
            return taskFailed(lockedTask, buildExceptionErrorConfig(ex).toConfig(cf));
        }

        boolean updated;
        if (subtaskAdded) {
            updated = lockedTask.setRunningToPlannedSuccessful(result);
        }
        else {
            updated = lockedTask.setRunningToShortCircuitSuccess(result);
        }

        noticeStatusPropagate();

        if (!updated) {
            // return value of setRunningToPlannedSuccessful or setRunningToShortCircuitSuccess
            // won't be false because this task is locked (won't be updated by other machines
            // concurrently) and confirmed that current state is RUNNING.
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
        List<ParameterUpdate> stores = sm.getStoreParams(parentsUpstreamChildrenFromFar);
        for (int si=0; si < parentsUpstreamChildrenFromFar.size(); si++) {
            ParameterUpdate stored = stores.get(si);
            long taskId = parentsUpstreamChildrenFromFar.get(si);
            int ei = parentsFromRoot.indexOf(taskId);
            if (ei >= 0) {
                // this is a parent task of the task
                Config exported = exports.get(ei);
                params.merge(exported);
            }
            stored.applyTo(params);
        }
        params.merge(task.getConfig().getExport());
    }

    private Optional<Long> addSubtasksIfNotEmpty(TaskControl lockedTask, Config subtaskConfig)
        throws TaskLimitExceededException
    {
        if (subtaskConfig.isEmpty()) {
            return Optional.absent();
        }

        WorkflowTaskList tasks = compiler.compileTasks(lockedTask.get().getFullName(), "^sub", subtaskConfig);
        if (tasks.isEmpty()) {
            return Optional.absent();
        }

        logger.trace("Adding sub tasks: {}", tasks);
        long rootTaskId = lockedTask.addGeneratedSubtasks(tasks, ImmutableList.of(), true, true);
        return Optional.of(rootTaskId);
    }

    public Optional<Long> addErrorTasksIfAny(TaskControl lockedTask, boolean isParentErrorPropagatedFromChildren, Function<Config, Config> errorBuilder)
        throws TaskLimitExceededException
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
        long rootTaskId = lockedTask.addGeneratedSubtasks(tasks, ImmutableList.of(), false);
        return Optional.of(rootTaskId);
    }

    private Optional<Long> addCheckTasksIfAny(TaskControl lockedTask, Optional<Long> upstreamTaskId)
        throws TaskLimitExceededException
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
        long rootTaskId = lockedTask.addGeneratedSubtasksWithoutLimit(tasks, ImmutableList.of(), false);
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
            lockedTask.addGeneratedSubtasksWithoutLimit(tasks, ImmutableList.of(), false);
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
            lockedTask.addGeneratedSubtasksWithoutLimit(tasks, ImmutableList.of(), false);
        }
    }
}
