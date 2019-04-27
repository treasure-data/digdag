package io.digdag.standards.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandStatus;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public class MockNonBlockingCommandExecutor
        implements CommandExecutor
{
    private static final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

    private final ObjectMapper mapper;

    public MockNonBlockingCommandExecutor(final ObjectMapper mapper)
    {
        this.mapper = mapper;
    }

    @Override
    public CommandStatus run(CommandContext context, CommandRequest request)
            throws IOException
    {
        final DefaultCommandStatus status = new DefaultCommandStatus();
        status.isFinished = false; // by default
        status.statusCode = 0;
        status.ioDirectory = request.getIoDirectory().toString();
        status.json = (ObjectNode) jsonNodeFactory.objectNode()
                .setAll(ImmutableMap.of(
                        "is_finished", jsonNodeFactory.booleanNode(status.isFinished),
                        "status_code", jsonNodeFactory.numberNode(status.statusCode),
                        "io_directory", jsonNodeFactory.textNode(status.ioDirectory)
                ));
        return status;
    }

    @Override
    public CommandStatus poll(CommandContext context, ObjectNode currentStatusJson)
            throws IOException
    {
        final DefaultCommandStatus status = new DefaultCommandStatus();
        status.isFinished = currentStatusJson.get("is_finished").booleanValue();
        status.statusCode = currentStatusJson.get("status_code").intValue();
        status.ioDirectory = currentStatusJson.get("io_directory").textValue();
        status.json = currentStatusJson;

        if (status.isFinished) {
            // write result to output.json
            final Path outputPath = context.getLocalProjectPath().resolve(status.getIoDirectory()).resolve("output.json");
            try (final OutputStream out = Files.newOutputStream(outputPath)) {
                mapper.writeValue(out, ImmutableMap.of("params",
                        ImmutableMap.of(
                                "subtask_config", Collections.emptyMap(),
                                "export_params", Collections.emptyMap(),
                                "store_params", Collections.emptyMap()
                        )));
            }
        }

        return status;
    }

    public static class DefaultCommandStatus
            implements CommandStatus
    {
        private boolean isFinished;
        private int statusCode;
        private String ioDirectory;
        private ObjectNode json;

        @Override
        public boolean isFinished()
        {
            return isFinished;
        }

        @Override
        public int getStatusCode()
        {
            return statusCode;
        }

        @Override
        public String getIoDirectory()
        {
            return ioDirectory;
        }

        @Override
        public ObjectNode toJson()
        {
            return json;
        }
    }
}
