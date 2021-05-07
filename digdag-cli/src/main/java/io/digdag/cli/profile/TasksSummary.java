package io.digdag.cli.profile;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.ImmutableArchivedTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.digdag.client.DigdagClient.objectMapper;

class TasksSummary
{
    private static final Logger logger = LoggerFactory.getLogger(TasksSummary.class);

    private static final Config EMPTY_CONFIG = new ConfigFactory(objectMapper()).create();

    @JsonProperty
    public final long attempts;
    @JsonProperty
    public final long totalTasks;
    @JsonProperty
    public final long totalRunTasks;
    @JsonProperty
    public final long totalSuccessTasks;
    @JsonProperty
    public final long totalErrorTasks;

    @JsonProperty
    public final ArchivedTask mostDelayedTask;
    @JsonProperty
    public final TasksStats startDelayMillis;
    @JsonProperty
    public final TasksStats execDurationMillis;

    public TasksSummary(
            long attempts,
            long totalTasks,
            long totalRunTasks,
            long totalSuccessTasks,
            long totalErrorTasks,
            ArchivedTask mostDelayedTask,
            TasksStats startDelayMillis,
            TasksStats execDurationMillis)
    {
        this.attempts = attempts;
        this.totalTasks = totalTasks;
        this.totalRunTasks = totalRunTasks;
        this.totalSuccessTasks = totalSuccessTasks;
        this.totalErrorTasks = totalErrorTasks;
        this.mostDelayedTask = mostDelayedTask;
        this.startDelayMillis = startDelayMillis;
        this.execDurationMillis = execDurationMillis;
    }

    interface Builder
    {
        void incrementAttempts();

        void incrementTotalTasks(long value);

        void incrementTotalRunTasks();

        void incrementTotalSuccessTasks();

        void incrementTotalErrorTasks();

        void addStartDelayMillis(long duration, Supplier<ArchivedTask> task);

        void addExecDurationMillis(long duration);

        default void updateWithTask(
                boolean isRoot,
                Map<Long, ArchivedTask> taskMap,
                Set<String> evaluatedTaskNames,
                ArchivedTask task)
        {
            // It's possible some old group tasks don't have `started_at`,
            // so skip if it doesn't exists
            // (This case corresponds to #2 in the comment above)
            if (!isRoot && task.getStartedAt().isPresent()) {
                incrementTotalRunTasks();
                // The stats of exec durations handle both group and non-group tasks for now.
                // There may be room to discuss about it.
                addExecDurationMillis(
                        Duration.between(task.getStartedAt().get(), task.getUpdatedAt()).toMillis());

                // To know the delay of a task, it's needed to choose the correct previous task
                // considering sequential execution and/or nested task.
                Optional<Instant> timestampWhenTaskIsReady = Optional.absent();
                if (task.getUpstreams().isEmpty()) {
                    // This task is the first child task of a group task
                    Optional<Long> parentId = task.getParentId();
                    if (parentId.isPresent()) {
                        ArchivedTask previousTask = taskMap.get(parentId.get());
                        // If the parent task is a pure group task having no actual tasks,
                        // `started_at` should be used as previous task's timestamp
                        // since `updated_at` is updated after all child tasks finish.
                        if (previousTask.getTaskType().isGroupingOnly()) {
                            // Just for old archived tasks
                            // (This case corresponds to #2 in the comment above)
                            if (previousTask.getStartedAt().isPresent()) {
                                // This task was executed after the previous group task started,
                                // so check the previous one's `started_at`
                                timestampWhenTaskIsReady = Optional.of(previousTask.getStartedAt().get());
                            }
                        }
                        else {
                            // Task start delay analysis should be skipped if the task is the first child of non GROUP_ONLY task
                            // (This case corresponds to #3 in the comment above)
                            timestampWhenTaskIsReady = Optional.absent();
                        }
                    }
                } else {
                    // This task is executed sequentially. Get the latest `updated_at` of upstream tasks.
                    //
                    // This way also can deal with `_parallel`'s `limit: N` option since
                    // tasks in the second or later parallel tasks set have
                    // previous parallel task IDs in `upstreams` field
                    long previousTaskId = task.getUpstreams().stream()
                            .max(Comparator.comparingLong(
                                    id -> taskMap.get(id).getUpdatedAt().toEpochMilli()))
                            .get();
                    ArchivedTask previousTask = taskMap.get(previousTaskId);
                    // Task start delay analysis should be skipped if the task was skipped in an retried attempt
                    // (This case corresponds to #4 in the comment above)
                    if (previousTask.getStartedAt().isPresent()) {
                        // This task was executed after the previous task finished,
                        // so check the previous one's `updated_at`
                        timestampWhenTaskIsReady = Optional.of(previousTask.getUpdatedAt());
                    }
                }

                // Task start delay analysis for this task should be skipped if it was dynamically generated.
                // (This case corresponds to #1 in the comment above)
                if (task.getFullName().endsWith("^check")
                        || task.getFullName().endsWith("^error")
                        || task.getFullName().endsWith("^sla")
                        || task.getFullName().endsWith("^failure-alert")) {
                    timestampWhenTaskIsReady = Optional.absent();
                }

                if (timestampWhenTaskIsReady.isPresent()
                        // Task start delay analysis for this task should be skipped if it was retried with `_retry`.
                        // (This case corresponds to #5 in the comment above)
                        && !evaluatedTaskNames.contains(task.getFullName())) {
                    long delayMillis = Duration.between(timestampWhenTaskIsReady.get(), task.getStartedAt().get()).toMillis();
                    addStartDelayMillis(delayMillis, () ->
                            ImmutableArchivedTask.builder()
                                    .attemptId(task.getAttemptId())
                                    .id(task.getId())
                                    .fullName(task.getFullName())
                                    .taskType(task.getTaskType())
                                    .parentId(task.getParentId())
                                    .error(task.getError())
                                    .report(task.getReport())
                                    .state(task.getState())
                                    .stateFlags(task.getStateFlags())
                                    .upstreams(task.getUpstreams())
                                    .resumingTaskId(task.getResumingTaskId())
                                    .config(task.getConfig())
                                    .retryCount(task.getRetryCount())
                                    .retryAt(task.getRetryAt())
                                    .startedAt(task.getStartedAt())
                                    .updatedAt(task.getUpdatedAt())
                                    .subtaskConfig(EMPTY_CONFIG)
                                    .exportParams(EMPTY_CONFIG)
                                    .storeParams(EMPTY_CONFIG)
                                    .stateParams(EMPTY_CONFIG)
                                    .build()
                    );
                }

                if (task.getState().isError()) {
                    incrementTotalErrorTasks();
                } else {
                    incrementTotalSuccessTasks();
                }
                evaluatedTaskNames.add(task.getFullName());
            }
        }

        // This method is called for each attempt and
        // accumulates stats in `builder`.
        default void updateWithTasks(List<ArchivedTask> originalTasks)
        {
            incrementAttempts();

            // Sort tasks by `id` just in case
            List<ArchivedTask> tasks = originalTasks.stream()
                    // Make it fail if unexpected overflow happens just in case
                    .sorted((a, b) -> Math.toIntExact(a.getId() - b.getId()))
                    .collect(Collectors.toList());

            // Create a task map for lookup by task id
            Map<Long, ArchivedTask> taskMap = new HashMap<>(tasks.size());
            for (ArchivedTask task : tasks) {
                taskMap.put(task.getId(), task);
            }

            incrementTotalTasks(tasks.size() - 1); // Remove a root task

            // Calculate the delays of task invocations
            boolean isRoot = true;
            HashSet<String> evaluatedTaskNames = new HashSet<>();
            for (ArchivedTask task : tasks) {
                updateWithTask(isRoot, taskMap, evaluatedTaskNames, task);
                isRoot = false;
            }
        }
    }

    static class DefaultBuilder
        implements Builder
    {
        long attempts;
        long totalTasks;
        long totalRunTasks;
        long totalSuccessTasks;
        long totalErrorTasks;

        long maxDelayMillis;
        ArchivedTask mostDelayedTask;

        final TasksStats.Builder startDelayMillis = new TasksStats.Builder();
        final TasksStats.Builder execDurationMillis = new TasksStats.Builder();

        @Override
        public void incrementAttempts()
        {
            attempts++;
        }

        @Override
        public void incrementTotalTasks(long value)
        {
            totalTasks += value;
        }

        @Override
        public void incrementTotalRunTasks()
        {
            totalRunTasks++;
        }

        @Override
        public void incrementTotalSuccessTasks()
        {
            totalSuccessTasks++;
        }

        @Override
        public void incrementTotalErrorTasks()
        {
            totalErrorTasks++;
        }

        @Override
        public void addStartDelayMillis(long duration, Supplier<ArchivedTask> task)
        {
            startDelayMillis.add(duration);
            if (duration > maxDelayMillis) {
                maxDelayMillis = duration;
                mostDelayedTask = task.get();
            }
        }

        @Override
        public void addExecDurationMillis(long duration)
        {
            execDurationMillis.add(duration);
        }

        TasksSummary build()
        {
            return new TasksSummary(
                    attempts,
                    totalTasks,
                    totalRunTasks,
                    totalSuccessTasks,
                    totalErrorTasks,
                    mostDelayedTask,
                    startDelayMillis.build(),
                    execDurationMillis.build()
            );
        }
    }

    @Override
    public String toString()
    {
        return "TasksSummary{" +
                "attempts=" + attempts +
                ", totalTasks=" + totalTasks +
                ", totalRunTasks=" + totalRunTasks +
                ", totalSuccessTasks=" + totalSuccessTasks +
                ", totalErrorTasks=" + totalErrorTasks +
                ", startDelayMillis=" + startDelayMillis +
                ", execDurationMillis=" + execDurationMillis +
                '}';
    }
}
