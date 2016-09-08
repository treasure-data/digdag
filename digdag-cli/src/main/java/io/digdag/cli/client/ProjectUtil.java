package io.digdag.cli.client;

import io.digdag.cli.CommandContext;
import io.digdag.client.api.RestProject;

import java.io.PrintStream;

class ProjectUtil
{
    static void showUploadedProject(CommandContext ctx, RestProject proj)
    {
        ctx.out().println("Uploaded:");
        ctx.out().println("  id: " + proj.getId());
        ctx.out().println("  name: " + proj.getName());
        ctx.out().println("  revision: " + proj.getRevision());
        ctx.out().println("  archive type: " + proj.getArchiveType());
        ctx.out().println("  project created at: " + proj.getCreatedAt());
        ctx.out().println("  revision updated at: " + proj.getUpdatedAt());
        ctx.out().println("");
        ctx.out().println("Use `" + ctx.programName() + " workflows` to show all workflows.");
    }
}
