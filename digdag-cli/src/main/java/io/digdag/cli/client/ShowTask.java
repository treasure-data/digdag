package io.digdag.cli.client;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.api.client.util.Throwables;
import com.google.common.base.Optional;
import com.google.common.math.Stats;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestTask;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.digdag.cli.SystemExitException.systemExit;

public class ShowTask
    extends ClientCommand
{
    @Parameter(names = {"-f", "--format"}, converter = FormatConverter.class)
    Format format = Format.TEXT;

    @Parameter(names = {"-t", "--type"}, converter = TypeConverter.class)
    Type type = Type.FULL;

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 1) {
            throw usage(null);
        }
        try {
            show(parseAttemptIdOrUsage(args.get(0)));
        }
        catch (Throwable e) { e.printStackTrace(); }
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " tasks <attempt-id>");
        err.println("  Options:");
        showCommonOptions();
        err.println("    -f, --format  FORMAT             Format: text or json (default: text)");
        err.println("    -t, --type    TYPE               Information type: full or summary (default: full)");
        return systemExit(error);
    }

    private void show(Id attemptId)
        throws Exception
    {
        DigdagClient client = buildClient();

        List<RestTask> tasks = client.getTasks(attemptId).getTasks();
        if (tasks.size() == 0) {
            client.getSessionAttempt(attemptId);  // throws exception if attempt doesn't exist
        }

        switch (type) {
            case FULL:
                format.printer.showTasks(this, tasks);
                break;
            case SUMMARY:
                format.printer.showSummary(this, TasksSummary.fromTasks(tasks));
                break;
        }
    }

    // Helper classes

    static class TasksSummary
    {
        @JsonProperty
        public final long totalTasks;
        @JsonProperty
        public final long totalInvokedTasks;
        @JsonProperty
        public final long totalSuccessTasks;

        @JsonProperty
        public final TasksStats startDelayMillis;
        @JsonProperty
        public final TasksStats execDurationOfGroupTasks;
        @JsonProperty
        public final TasksStats execDurationOfNonGroupTasks;

        @Override
        public String toString() {
            return "TasksSummary{" +
                    "totalTasks=" + totalTasks +
                    ", totalInvokedTasks=" + totalInvokedTasks +
                    ", totalSuccessTasks=" + totalSuccessTasks +
                    ", startDelayMillis=" + startDelayMillis +
                    ", execDurationOfGroupTasks=" + execDurationOfGroupTasks +
                    ", execDurationOfNonGroupTasks=" + execDurationOfNonGroupTasks +
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
            long totalInvokedTasks,
            long totalSuccessTasks,
            TasksStats startDelayMillis,
            TasksStats execDurationOfGroupTasks,
            TasksStats execDurationOfNonGroupTasks)
        {
            this.totalTasks = totalTasks;
            this.totalInvokedTasks = totalInvokedTasks;
            this.totalSuccessTasks = totalSuccessTasks;
            this.startDelayMillis = startDelayMillis;
            this.execDurationOfGroupTasks = execDurationOfGroupTasks;
            this.execDurationOfNonGroupTasks = execDurationOfNonGroupTasks;
        }

        public static TasksSummary fromTasks(List<RestTask> tasks)
        {
            Map<Long, RestTask> taskMap = new HashMap<>(tasks.size());
            for (RestTask task : tasks) {
                taskMap.put(task.getId().asLong(), task);
            }

            long totalTasks = tasks.size() - 1; // Remove a root task
            long totalSuccessTasks = 0;
            long totalInvokedTasks = 0;

            List<Long> startDelayMillisList = new ArrayList<>(tasks.size());
            List<Long> execTimeOfGroupTasksMillisList = new ArrayList<>(tasks.size());
            List<Long> execTimeOfNonGroupTasksMillisList = new ArrayList<>(tasks.size());

            // Calculate the delays of task invocations
            boolean isRoot = true;
            for (RestTask task : tasks) {
                if (!isRoot && task.getStartedAt().isPresent()) {
                    totalInvokedTasks++;
                    // Collect the metrics of group / non-group tasks separately
                    if (task.isGroup()) {
                        execTimeOfGroupTasksMillisList.add(
                                Duration.between(task.getStartedAt().get(), task.getUpdatedAt()).toMillis());
                    }
                    else {
                        execTimeOfNonGroupTasksMillisList.add(
                                Duration.between(task.getStartedAt().get(), task.getUpdatedAt()).toMillis());
                    }

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
            TasksStats statsOfExecTimeOfGroupTasksMillis = TasksStats.of(execTimeOfGroupTasksMillisList);
            TasksStats statsOfExecTimeOfNonGroupTasksMillis = TasksStats.of(execTimeOfNonGroupTasksMillisList);

            return new TasksSummary(
                    totalTasks,
                    totalInvokedTasks,
                    totalSuccessTasks,
                    statsOfStartDelayMillis,
                    statsOfExecTimeOfGroupTasksMillis,
                    statsOfExecTimeOfNonGroupTasksMillis);
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
            command.ln("   total invoked tasks: %s", tasksSummary.totalInvokedTasks);
            command.ln("   total success tasks: %s", tasksSummary.totalSuccessTasks);
            if (tasksSummary.startDelayMillis.stats.isPresent()) {
                command.ln("   start delay (ms):");
                command.ln("       average: %s", tasksSummary.startDelayMillis.mean());
                command.ln("       stddev: %s", tasksSummary.startDelayMillis.stdDev());
            }
            command.ln("   exec duration of group tasks (ms):");
            command.ln("       average: %s", tasksSummary.execDurationOfGroupTasks.mean());
            command.ln("       stddev: %s", tasksSummary.execDurationOfGroupTasks.stdDev());
            command.ln("   exec duration of non-group tasks (ms):");
            command.ln("       average: %s", tasksSummary.execDurationOfNonGroupTasks.mean());
            command.ln("       stddev: %s", tasksSummary.execDurationOfNonGroupTasks.stdDev());
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
}
