package io.digdag.cli.client;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.api.client.util.Throwables;
import io.digdag.cli.SystemExitException;
import io.digdag.cli.TimeUtil;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.Id;
import io.digdag.client.api.RestTask;

import java.util.List;

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
        showTasks(parseAttemptIdOrUsage(args.get(0)));
    }

    public SystemExitException usage(String error)
    {
        err.println("Usage: " + programName + " tasks <attempt-id>");
        err.println("  Options:");
        showCommonOptions();
        return systemExit(error);
    }

    private void showTasks(Id attemptId)
        throws Exception
    {
        DigdagClient client = buildClient();

        List<RestTask> tasks = client.getTasks(attemptId).getTasks();
        if (tasks.size() == 0) {
            client.getSessionAttempt(attemptId);  // throws exception if attempt doesn't exist
        }
        format.printer.showTasks(this, tasks);
    }

    private interface Printer
    {
        void showTasks(ClientCommand command, List<RestTask> tasks);
    }

    private static class TextPrinter
        implements Printer
    {
        @Override
        public void showTasks(ClientCommand command, List<RestTask> tasks) {
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
    }

    private static class JsonPrinter
        implements Printer
    {
        @Override
        public void showTasks(ClientCommand command, List<RestTask> tasks) {
            try {
                command.ln(command.objectMapper.writeValueAsString(tasks));
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

    static class FormatConverter implements IStringConverter<Format> {
        @Override
        public Format convert(String value) {
            return Format.valueOf(value.toUpperCase());
        }
    }

    enum Type
    {
        FULL, SUMMARY
    }

    static class TypeConverter implements IStringConverter<Type> {
        @Override
        public Type convert(String value) {
            return Type.valueOf(value.toUpperCase());
        }
    }
}
