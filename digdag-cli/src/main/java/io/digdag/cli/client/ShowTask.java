package io.digdag.cli.client;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.api.client.util.Throwables;
import com.google.common.base.Optional;
import com.google.common.math.Stats;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestTask;

import java.time.Duration;
import java.util.ArrayList;
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
        show(parseAttemptIdOrUsage(args.get(0)));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " tasks <attempt-id>");
        err.println("  Options:");
        showCommonOptions();
        // TODO
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

    static class TasksSummary
    {
        public final long totalTasks;
        public final long totalInvokedTasks;
        public final long totalSuccessTasks;

        public final long averageStartDelayMillis;
        public final long stdDevStartDelayMillis;

        public final long averageExecTimeMillis;
        public final long stdDevExecTimeMillis;

        public TasksSummary(
            long totalTasks,
            long totalInvokedTasks,
            long totalSuccessTasks,
            long averageStartDelayMillis,
            long stdDevStartDelayMillis,
            long averageExecTimeMillis,
            long stdDevExecTimeMillis)
        {
            this.totalTasks = totalTasks;
            this.totalInvokedTasks = totalInvokedTasks;
            this.totalSuccessTasks = totalSuccessTasks;
            this.averageStartDelayMillis = averageStartDelayMillis;
            this.stdDevStartDelayMillis = stdDevStartDelayMillis;
            this.averageExecTimeMillis = averageExecTimeMillis;
            this.stdDevExecTimeMillis = stdDevExecTimeMillis;
        }

        public static TasksSummary fromTasks(List<RestTask> tasks)
        {
            Map<String, RestTask> taskMap = new HashMap<>(tasks.size());
            for (RestTask task : tasks) {
                taskMap.put(task.getId().get(), task);
            }

            long totalTasks = tasks.size();
            long totalSuccessTasks = 0;
            long totalInvokedTasks = 0;

            List<Long> startDelayMillisList = new ArrayList<>(tasks.size());

            List<Long> execTimeMillisList = new ArrayList<>(tasks.size());

            for (RestTask task : tasks) {
                if (task.getStartedAt().isPresent()) {
                    totalInvokedTasks++;
                    execTimeMillisList.add(
                            Duration.between(task.getStartedAt().get(), task.getUpdatedAt()).toMillis());

                    Optional<Id> parentId = task.getParentId();
                    if (parentId.isPresent()) {
                        RestTask parent = taskMap.get(parentId.get().get());
                        if (parent != null) {
                            // FIXME: Parent's updated_at is updated after all children finished...
                            startDelayMillisList.add(
                                    Duration.between(parent.getUpdatedAt(), task.getStartedAt().get()).toMillis());
                        }
                    }
                    if (task.getState().equals("success")) {
                        totalSuccessTasks++;
                    }
                }
            }

            Stats statsOfStartDelayMillis = Stats.of(startDelayMillisList);
            Stats statsOfExecTimeMillis = Stats.of(execTimeMillisList);

            return new TasksSummary(
                    totalTasks,
                    totalInvokedTasks,
                    totalSuccessTasks,
                    (long) statsOfStartDelayMillis.mean(),
                    (long) statsOfStartDelayMillis.populationStandardDeviation(),
                    (long) statsOfExecTimeMillis.mean(),
                    (long) statsOfExecTimeMillis.populationStandardDeviation());
        }
    }

    private interface Printer
    {
        void showTasks(ClientCommand command, List<RestTask> tasks);

        void showSummary(ClientCommand command, TasksSummary tasksSummary);
    }

    private static class TextPrinter
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
            command.ln("   totalTasks: %s", tasksSummary.totalTasks);
            command.ln("   totalInvokedTasks: %s", tasksSummary.totalInvokedTasks);
            command.ln("   totalSuccessTasks: %s", tasksSummary.totalSuccessTasks);
            command.ln("   averageStartDelayMillis: %s", tasksSummary.averageStartDelayMillis);
            command.ln("   stdDevStartDelayMillis: %s", tasksSummary.stdDevStartDelayMillis);
            command.ln("   averageExecTimeMillis: %s", tasksSummary.averageExecTimeMillis);
            command.ln("   stdDevExecTimeMillis: %s", tasksSummary.stdDevExecTimeMillis);
        }
    }

    private static class JsonPrinter
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
