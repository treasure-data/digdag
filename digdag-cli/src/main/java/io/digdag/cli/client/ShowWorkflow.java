package io.digdag.cli.client;

import java.util.List;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestWorkflowDefinition;
import static io.digdag.cli.Main.systemExit;

public class ShowWorkflow
    extends ClientCommand
{
    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.isEmpty()) {
            showWorkflows();
        }
        else {
            throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag workflows");
        System.err.println("  Options:");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void showWorkflows()
        throws Exception
    {
        DigdagClient client = buildClient();
        List<RestWorkflowDefinition> defs = client.getWorkflowDefinitions();
        modelPrinter().printList(defs);
    }
}
