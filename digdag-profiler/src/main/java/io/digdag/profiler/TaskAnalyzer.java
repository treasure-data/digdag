package io.digdag.profiler;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.common.base.Optional;
import com.google.common.math.Stats;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.ConfigModule;
import io.digdag.core.database.DatabaseModule;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttemptWithSession;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class TaskAnalyzer
{
    private static final Logger logger = LoggerFactory.getLogger(TaskAnalyzer.class);

    private final Injector injector;

    public TaskAnalyzer(Injector injector)
    {
        this.injector = injector;
    }

    public void run(Instant createdFrom, Instant createdTo, int fetchedAttempts, int partitionSize)
    {
        TransactionManager tm = injector.getInstance(TransactionManager.class);
        ProjectStoreManager pm = injector.getInstance(ProjectStoreManager.class);
        SessionStoreManager sm = injector.getInstance(SessionStoreManager.class);

        AtomicLong counter = new AtomicLong();

        Map<Long, List<StoredSessionAttemptWithSession>> partitionedAttemptIds = tm.begin(() -> {
            List<StoredSessionAttemptWithSession> attempts =
                    sm.findFinishedAttemptsWithSessions(createdFrom, createdTo, 0, fetchedAttempts);
            return attempts.stream()
                    .collect(Collectors.groupingBy((attempt) -> counter.getAndIncrement() / partitionSize));
        });

        // Process attemptIds list from the beginning
        for (long attemptIdsGroup : partitionedAttemptIds.keySet().stream().sorted().collect(Collectors.toList())) {
            List<StoredSessionAttemptWithSession> attemptsWithSessions = partitionedAttemptIds.get(attemptIdsGroup);
            tm.begin(() -> {
                for (StoredSessionAttemptWithSession attemptWithSession : attemptsWithSessions) {
                    // TODO: Reduce these round-trips with database to improve the performance
                    int projectId = attemptWithSession.getSession().getProjectId();
                    int siteId;
                    try {
                        siteId = pm.getProjectByIdInternal(projectId).getSiteId();
                    } catch (ResourceNotFoundException e) {
                        logger.error(String.format("Can't find the project: %d. Just skipping it...", projectId), e);
                        continue;
                    }
                    List<ArchivedTask> tasks = sm.getSessionStore(siteId).getTasksOfAttempt(attemptWithSession.getId());
                    logger.info("Attempt ID: {}, Summary: {}", attemptWithSession.getId(), TasksSummary.fromTasks(tasks));
                }
                return null;
            });
        }
    }

    // Helper classes

    static class TasksSummary
    {
        @JsonProperty
        public final long totalTasks;
        @JsonProperty
        public final long totalRunTasks;
        @JsonProperty
        public final long totalSuccessTasks;
        @JsonProperty
        public final long totalErrorTasks;

        @JsonProperty
        public final TasksStats startDelayMillis;
        @JsonProperty
        public final TasksStats execDuration;

        @Override
        public String toString() {
            return "TasksSummary{" +
                    "totalTasks=" + totalTasks +
                    ", totalRunTasks=" + totalRunTasks +
                    ", totalSuccessTasks=" + totalSuccessTasks +
                    ", totalErrorTasks=" + totalErrorTasks +
                    ", startDelayMillis=" + startDelayMillis +
                    ", execDuration=" + execDuration +
                    '}';
        }

        static class NullableLong
        {
            @JsonProperty
            final Optional<Long> value;

            NullableLong(Optional<Long> value)
            {
                this.value = value;
            }

            @Override
            public String toString()
            {
                if (value.isPresent()) {
                    return value.get().toString();
                }
                else {
                    return "N/A";
                }
            }
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
                }
                else {
                    return new TasksStats(Optional.of(Stats.of(values)));
                }
            }

            NullableLong mean()
            {
                return new NullableLong(
                        stats.transform(x -> Double.valueOf(x.mean()).longValue()));
            }

            NullableLong stdDev()
            {
                return new NullableLong(
                        stats.transform(x -> Double.valueOf(x.populationStandardDeviation()).longValue()));
            }

            @Override
            public String toString() {
                return "TasksStats{" +
                        "stats=" + stats +
                        '}';
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
                jsonGenerator.writeObjectField("average", tasksStats.mean());
                jsonGenerator.writeObjectField("stddev", tasksStats.stdDev());
                jsonGenerator.writeEndObject();
            }
        }

        public TasksSummary(
            long totalTasks,
            long totalRunTasks,
            long totalSuccessTasks,
            long totalErrorTasks,
            TasksStats startDelayMillis,
            TasksStats execDuration)
        {
            this.totalTasks = totalTasks;
            this.totalRunTasks = totalRunTasks;
            this.totalSuccessTasks = totalSuccessTasks;
            this.totalErrorTasks = totalErrorTasks;
            this.startDelayMillis = startDelayMillis;
            this.execDuration = execDuration;
        }

        public static TasksSummary fromTasks(List<ArchivedTask> tasks)
        {
            Map<Long, ArchivedTask> taskMap = new HashMap<>(tasks.size());
            for (ArchivedTask task : tasks) {
                taskMap.put(task.getId(), task);
            }

            long totalTasks = tasks.size() - 1; // Remove a root task
            long totalSuccessTasks = 0;
            long totalErrorTasks = 0;
            long totalRunTasks = 0;

            List<Long> startDelayMillisList = new ArrayList<>(tasks.size());
            List<Long> execTimeMillisList = new ArrayList<>(tasks.size());

            // Calculate the delays of task invocations
            boolean isRoot = true;
            for (ArchivedTask task : tasks) {
                if (!isRoot && task.getStartedAt().isPresent()) {
                    totalRunTasks++;
                    execTimeMillisList.add(
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
                    }
                    else {
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
                        startDelayMillisList.add(
                                Duration.between(timestampWhenTaskIsReady.get(), task.getStartedAt().get()).toMillis());
                    }
                    if (!task.getState().isError()) {
                        totalSuccessTasks++;
                    }
                }
                isRoot = false;
            }

            TasksStats statsOfStartDelayMillis = TasksStats.of(startDelayMillisList);
            TasksStats statsOfExecTime = TasksStats.of(execTimeMillisList);

            return new TasksSummary(
                    totalTasks,
                    totalRunTasks,
                    totalSuccessTasks,
                    totalErrorTasks,
                    statsOfStartDelayMillis,
                    statsOfExecTime);
        }
    }
}
