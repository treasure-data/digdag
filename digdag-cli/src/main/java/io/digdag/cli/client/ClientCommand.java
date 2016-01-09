package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import io.digdag.cli.Main;
import io.digdag.cli.Command;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;

public abstract class ClientCommand
    extends Command
{
    @Parameter(names = {"-e", "--endpoint"}, required = true)
    protected String endpoint;

    protected DigdagClient buildClient()
    {
        String[] fragments = endpoint.split(":", 2);
        String host;
        int port;
        if (fragments.length == 1) {
            host = fragments[0];
            port = 80;
        }
        else {
            host = fragments[0];
            port = Integer.parseInt(fragments[1]);
        }

        return DigdagClient.builder()
            .host(host)
            .port(port)
            .build();
    }

    public static void showCommonOptions()
    {
        System.err.println("    -h, --host HOST[:PORT]           HTTP endpoint");
        Main.showCommonOptions();
    }

    public long parseLongOrUsage(String arg)
        throws SystemExitException
    {
        try {
            return Long.parseLong(args.get(0));
        }
        catch (NumberFormatException ex) {
            throw usage(ex.getMessage());
        }
    }

    protected ModelPrinter modelPrinter()
    {
        return new ModelPrinter();
    }
}
