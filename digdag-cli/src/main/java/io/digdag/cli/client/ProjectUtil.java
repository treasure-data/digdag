package io.digdag.cli.client;

import io.digdag.client.api.RestProject;

import java.io.PrintStream;

class ProjectUtil
{
    static void showUploadedProject(PrintStream out, RestProject proj)
    {
        out.println("Uploaded:");
        out.println("  id: " + proj.getId());
        out.println("  name: " + proj.getName());
        out.println("  revision: " + proj.getRevision());
        out.println("  archive type: " + proj.getArchiveType());
        out.println("  project created at: " + proj.getCreatedAt());
        out.println("  revision updated at: " + proj.getUpdatedAt());
        out.println("");
        out.println("Use `digdag workflows` to show all workflows.");
    }
}
