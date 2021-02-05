package io.digdag.profiler;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.common.base.Optional;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.digdag.client.api.JacksonTimeModule;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.core.DigdagEmbed;
import io.digdag.core.config.ConfigModule;
import io.digdag.core.database.DatabaseModule;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionWithLastAttempt;

import java.util.List;

public class TaskAnalyzer
{
    private final ConfigElement configElement;

    public TaskAnalyzer(ConfigElement configElement)
    {
        this.configElement = configElement;
    }

    public void run()
        throws Exception
    {
        Injector injector = Guice.createInjector(
                new ObjectMapperModule()
                        .registerModule(new GuavaModule())
                        .registerModule(new JacksonTimeModule()),
                new DatabaseModule(false),
                new ConfigModule(),
                (binder) -> {
                    binder.bind(ConfigElement.class).toInstance(configElement);
                    binder.bind(Config.class).toProvider(DigdagEmbed.SystemConfigProvider.class);
                }

        );
        TransactionManager tm = injector.getInstance(TransactionManager.class);
        SessionStoreManager sm = injector.getInstance(SessionStoreManager.class);

        List<StoredSessionWithLastAttempt> sessions = tm.begin(() -> {
            SessionStore ss = sm.getSessionStore(0);
            return ss.getSessions(10, Optional.absent(), () -> "true");
        });
    }

    /*
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

        public static TasksSummary fromTasks(List<RestTask> tasks)
        {
            Map<Long, RestTask> taskMap = new HashMap<>(tasks.size());
            for (RestTask task : tasks) {
                taskMap.put(task.getId().asLong(), task);
            }

            long totalTasks = tasks.size() - 1; // Remove a root task
            long totalSuccessTasks = 0;
            long totalErrorTasks = 0;
            long totalRunTasks = 0;

            List<Long> startDelayMillisList = new ArrayList<>(tasks.size());
            List<Long> execTimeMillisList = new ArrayList<>(tasks.size());

            // Calculate the delays of task invocations
            boolean isRoot = true;
            for (RestTask task : tasks) {
                if (!isRoot && task.getStartedAt().isPresent()) {
                    totalRunTasks++;
                    execTimeMillisList.add(
                            Duration.between(task.getStartedAt().get(), task.getUpdatedAt()).toMillis());

                    // To know the delay of a task, it's needed to choose the correct previous task
                    // considering sequential execution and/or nested task.
                    Optional<Instant> timestampWhenTaskIsReady = Optional.absent();
                    if (task.getUpstreams().isEmpty()) {
                        // This task is the first child task of a group task
                        Optional<Id> parentId = task.getParentId();
                        if (parentId.isPresent()) {
                            RestTask previousTask = taskMap.get(parentId.get().asLong());
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
                                id -> taskMap.get(id.asLong()).getUpdatedAt().toEpochMilli()))
                            .get().asLong();
                        RestTask previousTask = taskMap.get(previousTaskId);
                        // This task was executed after the previous task finished,
                        // so check the previous one's `updated_at`
                        timestampWhenTaskIsReady = Optional.of(previousTask.getUpdatedAt());
                    }
                    if (timestampWhenTaskIsReady.isPresent()) {
                        startDelayMillisList.add(
                                Duration.between(timestampWhenTaskIsReady.get(), task.getStartedAt().get()).toMillis());
                    }
                    if (task.getState().equals("success")) {
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

    interface Printer
    {
        void showTasks(ClientCommand command, List<RestTask> tasks);

        void showSummary(ClientCommand command, TasksSummary tasksSummary);
    }

    static class TextPrinter
        implements Printer
    {
        @Override
        public void showTasks(ClientCommand command, List<RestTask> tasks)
        {
            for (RestTask task : tasks) {
                command.ln("   id: %s", task.getId());
                command.ln("   name: %s", task.getFullName());
                command.ln("   state: %s", task.getState());
                command.ln("   started: %s", task.getStartedAt().transform(TimeUtil::formatTime).or(""));
                command.ln("   updated: %s", TimeUtil.formatTime(task.getUpdatedAt()));
                command.ln("   config: %s", task.getConfig());
                command.ln("   parent: %s", task.getParentId().orNull());
                command.ln("   upstreams: %s", task.getUpstreams());
                command.ln("   export params: %s", task.getExportParams());
                command.ln("   store params: %s", task.getStoreParams());
                command.ln("   state params: %s", task.getStateParams());
                command.ln("");
            }

            command.ln("%d entries.", tasks.size());
        }

        @Override
        public void showSummary(ClientCommand command, TasksSummary tasksSummary)
        {
            command.ln("   total tasks: %s", tasksSummary.totalTasks);
            command.ln("   total run tasks: %s", tasksSummary.totalRunTasks);
            command.ln("   total success tasks: %s", tasksSummary.totalSuccessTasks);
            command.ln("   total error tasks: %s", tasksSummary.totalErrorTasks);
            if (tasksSummary.startDelayMillis.stats.isPresent()) {
                command.ln("   start delay (ms):");
                command.ln("       average: %s", tasksSummary.startDelayMillis.mean());
                command.ln("       stddev: %s", tasksSummary.startDelayMillis.stdDev());
            }
            command.ln("   exec duration (ms):");
            command.ln("       average: %s", tasksSummary.execDuration.mean());
            command.ln("       stddev: %s", tasksSummary.execDuration.stdDev());
        }
    }

    static class JsonPrinter
        implements Printer
    {
        @Override
        public void showTasks(ClientCommand command, List<RestTask> tasks)
        {
            try {
                command.ln(command.objectMapper.writeValueAsString(tasks));
            }
            catch (JsonProcessingException e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void showSummary(ClientCommand command, TasksSummary tasksSummary)
        {
            try {
                command.ln(command.objectMapper.writeValueAsString(tasksSummary));
            }
            catch (JsonProcessingException e) {
                Throwables.propagate(e);
            }
        }
    }

    enum Format
    {
        JSON(new JsonPrinter()), TEXT(new TextPrinter());

        Printer printer;

        Format(Printer printer)
        {
            this.printer = printer;
        }
    }

    static class FormatConverter implements IStringConverter<Format>
    {
        @Override
        public Format convert(String value)
        {
            return Format.valueOf(value.toUpperCase());
        }
    }

    enum Type
    {
        FULL, SUMMARY
    }

    static class TypeConverter implements IStringConverter<Type>
    {
        @Override
        public Type convert(String value)
        {
            return Type.valueOf(value.toUpperCase());
        }
    }
     */
}
