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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.digdag.client.DigdagClient.objectMapper;

class TasksSummary
{
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
    public final TasksStats execDuration;

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
        this.execDuration = execDurationMillis;
    }

    static class Builder
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

    static void updateBuilderWithTasks(
            List<ArchivedTask> tasks,
            Builder builder)
    {
        builder.attempts++;

        Map<Long, ArchivedTask> taskMap = new HashMap<>(tasks.size());
        for (ArchivedTask task : tasks) {
            taskMap.put(task.getId(), task);
        }

        builder.totalTasks += tasks.size() - 1; // Remove a root task

        // Calculate the delays of task invocations
        Config emptyConfig = new ConfigFactory(objectMapper()).create();
        boolean isRoot = true;
        for (ArchivedTask task : tasks) {
            if (!isRoot && task.getStartedAt().isPresent()) {
                builder.totalRunTasks++;
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
                        // Just for backward compatibility
                        if (previousTask.getStartedAt().isPresent()) {
                            // This task was executed after the previous group task started,
                            // so check the previous one's `started_at`
                            timestampWhenTaskIsReady = Optional.of(previousTask.getStartedAt().get());
                        }
                    }
                } else {
                    // This task is executed sequentially. Get the latest `updated_at` of upstream tasks.
                    long previousTaskId = task.getUpstreams().stream()
                            .max(Comparator.comparingLong(
                                    id -> taskMap.get(id).getUpdatedAt().toEpochMilli()))
                            .get();
                    ArchivedTask previousTask = taskMap.get(previousTaskId);
                    // This task was executed after the previous task finished,
                    // so check the previous one's `updated_at`
                    timestampWhenTaskIsReady = Optional.of(previousTask.getUpdatedAt());
                }

                if (timestampWhenTaskIsReady.isPresent()) {
                    long delayMillis = Duration.between(timestampWhenTaskIsReady.get(), task.getStartedAt().get()).toMillis();
                    builder.startDelayMillis.add(delayMillis);
                    if (delayMillis > builder.maxDelayMillis) {
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
                                .subtaskConfig(emptyConfig)
                                .exportParams(emptyConfig)
                                .storeParams(emptyConfig)
                                .stateParams(emptyConfig)
                                .build();
                        builder.maxDelayMillis = delayMillis;
                    }
                }

                if (task.getState().isError()) {
                    // TODO: This includes GROUP_ERROR. Is it okay...?
                    builder.totalErrorTasks++;
                } else {
                    builder.totalSuccessTasks++;
                }
            }
            isRoot = false;
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
                ", execDuration=" + execDuration +
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
