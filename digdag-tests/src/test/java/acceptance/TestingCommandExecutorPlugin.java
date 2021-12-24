package acceptance;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorFactory;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.Plugin;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class TestingCommandExecutorPlugin
        implements Plugin
{
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type)
    {
        if (type == CommandExecutorFactory.class) {
            return TestingCommandExecutorFactory.class.asSubclass(type);
        }
        else {
            return null;
        }
    }

    static class TestingCommandExecutorFactory
            implements CommandExecutorFactory
    {

        private final CommandLogger clog;

        @Inject
        public TestingCommandExecutorFactory(CommandLogger clog)
        {
            this.clog = clog;
        }

        public String getType()
        {
            return "testing";
        }

        @Override
        public CommandExecutor newCommandExecutor()
        {
            return new TestingCommandExecutor(clog);
        }
    }

    static class TestingCommandExecutor
            implements CommandExecutor
    {
        private final CommandLogger clog;

        public TestingCommandExecutor(CommandLogger clog)
        {
            this.clog = clog;
        }

        @Override
        public CommandStatus run(CommandContext context, CommandRequest request)
                throws IOException
        {
            String testStr = "===TestingCommandExecutor====\n";
            InputStream is = new ByteArrayInputStream(testStr.getBytes(StandardCharsets.UTF_8));
            clog.copy(is, System.out);
            return new TestingCommandStatus(0, request.getIoDirectory().toString());
        }

        @Override
        public CommandStatus poll(CommandContext context, ObjectNode previousStatusJson)
                throws IOException
        {
            throw new UnsupportedOperationException("This method should not be called.");
        }
    }

    static class TestingCommandStatus
        implements CommandStatus
    {
        private final int statusCode;
        private final String ioDirectory;

        TestingCommandStatus(final int statusCode, final String ioDirectory)
        {
            this.statusCode = statusCode;
            this.ioDirectory = ioDirectory;
        }

        @Override
        public boolean isFinished()
        {
            return true;
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
            throw new UnsupportedOperationException();
        }
    }
}
