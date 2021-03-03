package io.digdag.profiler;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Optional;
import com.google.common.math.Stats;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.ImmutableArchivedTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.digdag.client.DigdagClient.objectMapper;

/**
 * Represents the summary of task profiling and has capability of calculating task start delays and execution durations.
 *
 * Current implementation has a few limitations of task start delay profiling:
 *
 * 1. Skip whole task profiling when an attempt:
 *   1. contains `error` tasks
 *
 *   2. contains `_checked` directive
 *
 * 2. Skip a task profiling when the task doesn't have `started_at`
 *    (this can happen with old archived tasks generated by old implementation)
 *
 * 3. Skip a task profiling when the task has no upstream tasks and:
 *   1. has a parent that's not GROUP_ONLY task type (e.g. the first child task in a `for_each` operator)
 *
 *   2. has a parent that doesn't have `started_at`
 *      (this can happen with old archived tasks generated by old implementation)
 *
 * 4. Skip a task profiling when the task has any upstream tasks
 *    and the upstream tasks don't have `started_at` (this happens when the attempt is started by `retry --resume`)
 *
 * 5. Skip a task profiling when the task is retried one because a retried task is directly connected to the parent task
 *    and the `started_at` is recorded after proceeding tasks fail
 *
 *
 * For 1, the reason of this limitation is `failure-alert`, `_error` and `_check` are dynamically generated to the parent task
 * after preceding tasks take some time and we can't tell it apart from actual delay.
 *
 * For 2 and 3.2, these don't matter since they won't happen with recent archived tasks generated by recent implementation.
 *
 * For 3.1, the reason of this limitation is some operators start child tasks
 * after its own operation finishes (e.g. `td_for_each` operator) and we can't tell it apart from actual delay.
 *
 * For 4, the behavior that skipped tasks in a retried attempt have empty `started_at` itself seems unexpected.
 * It may be fixed in the future.
 *
 * For 5, the behavior that retried tasks have later `started_at` seems reasonable.
 * This limitation won't be removed for a while...
 *
 */
class TasksSummary
{
    private static final Logger logger = LoggerFactory.getLogger(TasksSummary.class);

    private static final Config EMPTY_CONFIG = new ConfigFactory(objectMapper()).create();

    @JsonProperty
    public final long attempts;
    @JsonProperty
    public final long attemptsStartDelayProfileSkipped;
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
            long attemptsStartDelayProfileSkipped,
            long totalTasks,
            long totalRunTasks,
            long totalSuccessTasks,
            long totalErrorTasks,
            ArchivedTask mostDelayedTask,
            TasksStats startDelayMillis,
            TasksStats execDurationMillis)
    {
        this.attempts = attempts;
        this.attemptsStartDelayProfileSkipped = attemptsStartDelayProfileSkipped;
        this.totalTasks = totalTasks;
        this.totalRunTasks = totalRunTasks;
        this.totalSuccessTasks = totalSuccessTasks;
        this.totalErrorTasks = totalErrorTasks;
        this.mostDelayedTask = mostDelayedTask;
        this.startDelayMillis = startDelayMillis;
        this.execDurationMillis = execDurationMillis;
    }

    static class Builder
    {
        long attempts;
        long attemptsStartDelayProfileSkipped;
        long totalTasks;
        long totalRunTasks;
        long totalSuccessTasks;
        long totalErrorTasks;

        long maxDelayMillis;
        ArchivedTask mostDelayedTask;

        final TasksStats.Builder startDelayMillis = new TasksStats.Builder();
        final TasksStats.Builder execDurationMillis = new TasksStats.Builder();

        TasksSummary build()
        {
            return new TasksSummary(
                    attempts,
                    attemptsStartDelayProfileSkipped,
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

    // With current information stored in `task_archives.tasks` columns,
    // it's hard to filter out dynamically added tasks by `_error` or notifications
    // that introduce unexpected larger start delays.
    // So digdag-profiler decides not to profile start delays if an attempt has any error task.
    //
    // `_check` directive also dynamically generates tasks, so attempts containing the directive
    // is also skipped.
    static boolean shouldProfileStartDelay(List<ArchivedTask> tasks)
    {
        for (ArchivedTask task : tasks) {
            if (task.getState().isError()) {
                // This block takes care of both cases of `_error` and notifications
                // (This case corresponds to #1.1 in the comment above)
                logger.info("This attempt {} contains failed tasks and the profile of start delays won't be executed on this attempt", task.getAttemptId());
                return false;
            }
            else if (!task.getConfig().getCheckConfig().isEmpty()) {
                // (This case corresponds to #1.2 in the comment above)
                logger.info("This attempt {} contains `_check` directive and the profile of start delays won't be executed on this attempt", task.getAttemptId());
                return false;
            }
        }
        return true;
    }

    static void updateBuilderWithTask(
            Builder builder,
            boolean shouldProfileStartDelay,
            boolean isRoot,
            Map<Long, ArchivedTask> taskMap,
            ArchivedTask task)
    {
        // It's possible some old group tasks don't have `started_at`,
        // so skip if it doesn't exists
        // (This case corresponds to #2 in the comment above)
        if (!isRoot && task.getStartedAt().isPresent()) {
            builder.totalRunTasks++;
            // The stats of exec durations handle both group and non-group tasks for now.
            // There may be room to discuss about it.
            builder.execDurationMillis.add(
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
                        // Just for backward compatibility
                        // (This case corresponds to #2 in the comment above)
                        if (previousTask.getStartedAt().isPresent()) {
                            // This task was executed after the previous group task started,
                            // so check the previous one's `started_at`
                            timestampWhenTaskIsReady = Optional.of(previousTask.getStartedAt().get());
                        }
                    }
                    else {
                        // Give up profiling start delay if the parent is not purely group task
                        // since the archived task doesn't have information about
                        // when the succeeding task is ready....
                        // timestampWhenTaskIsReady = Optional.of(previousTask.getUpdatedAt());
                        // (This case corresponds to #3.1 in the comment above)
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
                // In sequential tasks, previous task's `started_at` isn't needed,
                // but in current implementation of attempt retry with `--resume`,
                // previous task's `updated_at` is the same as the old task's one of the failed attempt
                // and it doesn't work for this profiling.
                // So, skip this kind of retried previous tasks
                if (previousTask.getStartedAt().isPresent()) {
                    // This task was executed after the previous task finished,
                    // so check the previous one's `updated_at`
                    // (This case corresponds to #4 in the comment above)
                    timestampWhenTaskIsReady = Optional.of(previousTask.getUpdatedAt());
                }
            }

            if (shouldProfileStartDelay
                    && timestampWhenTaskIsReady.isPresent()
                    // Task start delay profiling on retried task is skipped
                    // (This case corresponds to #5 in the comment above)
                    && task.getRetryCount() == 0) {
                long delayMillis = Duration.between(timestampWhenTaskIsReady.get(), task.getStartedAt().get()).toMillis();
                builder.startDelayMillis.add(delayMillis);
                if (delayMillis > builder.maxDelayMillis) {
                    // Mask some unnecessary fields
                    builder.mostDelayedTask = ImmutableArchivedTask.builder()
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
                            .build();
                    builder.maxDelayMillis = delayMillis;
                }
            }

            if (task.getState().isError()) {
                builder.totalErrorTasks++;
            } else {
                builder.totalSuccessTasks++;
            }
        }
    }

    // This method is called for each attempt and
    // accumulates stats in `builder`.
    static void updateBuilderWithTasks(
            Builder builder,
            List<ArchivedTask> originalTasks)
    {
        boolean shouldProfileStartDelay = shouldProfileStartDelay(originalTasks);
        builder.attempts++;
        if (!shouldProfileStartDelay) {
            builder.attemptsStartDelayProfileSkipped++;
        }

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

        builder.totalTasks += tasks.size() - 1; // Remove a root task

        // Calculate the delays of task invocations
        boolean isRoot = true;
        for (ArchivedTask task : tasks) {
            updateBuilderWithTask(builder, shouldProfileStartDelay, isRoot, taskMap, task);
            isRoot = false;
        }
    }

    @Override
    public String toString()
    {
        return "TasksSummary{" +
                "attempts=" + attempts +
                ", attemptsStartDelayProfileSkipped=" + attemptsStartDelayProfileSkipped +
                ", totalTasks=" + totalTasks +
                ", totalRunTasks=" + totalRunTasks +
                ", totalSuccessTasks=" + totalSuccessTasks +
                ", totalErrorTasks=" + totalErrorTasks +
                ", startDelayMillis=" + startDelayMillis +
                ", execDurationMillis=" + execDurationMillis +
                '}';
    }

    @JsonSerialize(using = TasksStatsSerializer.class)
    static class TasksStats
    {
        final Optional<Stats> stats;

        TasksStats(Optional<Stats> stats)
        {
            this.stats = stats;
        }

        static TasksStats of(Collection<Long> values)
        {
            if (values.isEmpty()) {
                return new TasksStats(Optional.absent());
            } else {
                return new TasksStats(Optional.of(Stats.of(values)));
            }
        }

        Long min()
        {
            return stats.transform(x -> Double.valueOf(x.min()).longValue()).orNull();
        }

        Long max()
        {
            return stats.transform(x -> Double.valueOf(x.max()).longValue()).orNull();
        }

        Long mean()
        {
            return stats.transform(x -> Double.valueOf(x.mean()).longValue()).orNull();
        }

        Long stdDev()
        {
            return stats.transform(x -> Double.valueOf(x.populationStandardDeviation()).longValue()).orNull();
        }

        @Override
        public String toString()
        {
            return "TasksStats{" +
                    "stats=" + stats +
                    '}';
        }

        static class Builder
        {
            private final List<Long> items = new ArrayList<>();

            void add(long item)
            {
                items.add(item);
            }

            TasksStats build()
            {
                return TasksStats.of(items);
            }
        }
    }

    static class TasksStatsSerializer
            extends StdSerializer<TasksStats>
    {
        protected TasksStatsSerializer()
        {
            super(TasksStats.class);
        }

        @Override
        public void serialize(TasksStats tasksStats, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField("min", tasksStats.min());
            jsonGenerator.writeObjectField("max", tasksStats.max());
            jsonGenerator.writeObjectField("average", tasksStats.mean());
            jsonGenerator.writeObjectField("stddev", tasksStats.stdDev());
            jsonGenerator.writeEndObject();
        }
    }
}
