package io.digdag.cli.client;

import com.beust.jcommander.Parameter;
import io.digdag.cli.CommandContext;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestProject;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static io.digdag.cli.SystemExitException.systemExit;
import static io.digdag.cli.client.ProjectUtil.showUploadedProject;

public class Upload
    extends ClientCommand
{
    @Parameter(names = {"-r", "--revision"})
    String revision = null;

    public Upload(CommandContext context)
    {
        super(context);
    }

    @Override
    public void mainWithClientException()
        throws Exception
    {
        if (args.size() != 2) {
            throw usage(null);
        }
        upload(args.get(0), args.get(1));
    }

    public SystemExitException usage(String error)
    {
        ctx.err().println("Usage: " + ctx.programName() + " upload <path.tar.gz> <project>");
        ctx.err().println("  Options:");
        ctx.err().println("    -r, --revision REVISION          specific revision name instead of auto-generated UUID");
        //ctx.err().println("        --time-revision              use current time as the revision name");
        showCommonOptions();
        return systemExit(error);
    }

    private void upload(String path, String projName)
        throws IOException, SystemExitException
    {
        DigdagClient client = buildClient();
        if (revision == null) {
            revision = generateDefaultRevisionName();
        }
        RestProject proj = client.putProjectRevision(projName, revision, new File(path));
        showUploadedProject(ctx, proj);
    }

    public static String generateDefaultRevisionName()
    {
        return UUID.randomUUID().toString();
    }
}
