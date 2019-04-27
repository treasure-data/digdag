package io.digdag.standards.command.kubernetes;

import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandRequest;

import java.io.IOException;
import java.util.List;

public interface KubernetesClient
        extends AutoCloseable
{
    KubernetesClientConfig getConfig();

    Pod runPod(CommandContext context, CommandRequest request, String name, List<String> commands, List<String> arguments);

    Pod pollPod(String podName);

    boolean deletePod(String podName);

    boolean isWaitingContainerCreation(Pod pod);

    String getLog(String podName, long offset) throws IOException;

    @Override
    void close();
}
