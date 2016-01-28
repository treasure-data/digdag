package io.digdag.cli.client;

import java.util.List;
import com.beust.jcommander.Parameter;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestRepository;
import io.digdag.client.api.RestWorkflowDefinition;
import static io.digdag.cli.Main.systemExit;

public class ShowWorkflow
    extends ClientCommand
{
    @Parameter(names = {"-r", "--repo"})
    String repoName = null;

    @Override
    public void mainWithClientException()
        throws Exception
    {
        switch (args.size()) {
        case 0:
            showWorkflows();
            break;
        case 1:
            showWorkflowDetails(args.get(0));
            break;
        default:
            throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag workflows [+name]");
        System.err.println("  Options:");
        System.err.println("    -r, --repository NAME            repository name");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void showWorkflows()
        throws Exception
    {
        DigdagClient client = buildClient();

        for (RestRepository repo : client.getRepositories()) {
            // TODO catch and ignore not-found exceptions
            ln("  %s", repo.getName());
            if (repoName == null || repoName.equals(repo.getName())) {
                for (RestWorkflowDefinition def : client.getWorkflowDefinitions(repo.getId())) {
                    ln("    %s", def.getName());
                }
                if (repoName != null) {
                    return;
                }
            }
        }
        if (repoName != null) {
            throw systemExit("Repository " + repoName + " does not exist.");
        }
        ln("");
        System.err.println("Use `digdag workflows +NAME` to show details.");
    }

    public void showWorkflowDetails(String defName)
        throws Exception
    {
        DigdagClient client = buildClient();

        for (RestRepository repo : client.getRepositories()) {
            // TODO catch and ignore not-found exceptions
            if (repoName == null || repoName.equals(repo.getName())) {
                for (RestWorkflowDefinition def : client.getWorkflowDefinitions(repo.getId())) {
                    if (defName.equals(def.getName())) {
                        String yaml = yamlMapper().toYaml(def.getConfig());
                        ln("---");
                        ln("%s", yaml);
                        return;
                    }
                }
                if (repoName != null) {
                    break;
                }
            }
        }
        throw systemExit("Workflow definition '" + defName + "' does not exist.");
    }
}
