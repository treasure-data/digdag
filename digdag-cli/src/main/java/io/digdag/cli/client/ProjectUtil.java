package io.digdag.cli.client;

import io.digdag.cli.EntityPrinter;
import io.digdag.cli.OutputFormat;
import io.digdag.client.api.RestProject;

import java.io.IOException;
import java.io.PrintStream;

class ProjectUtil
{
    static void showUploadedProject(PrintStream out, RestProject proj, OutputFormat format)
            throws IOException
    {
        EntityPrinter<RestProject> printer = new EntityPrinter<>();

        printer.field("id", p -> Long.toString(p.getId()));
        printer.field("name", RestProject::getName);
        printer.field("revision", RestProject::getRevision);
        printer.field("archive type", RestProject::getArchiveType);
        printer.field("project created at", p -> p.getCreatedAt().toString());
        printer.field("revision updated at", p -> p.getUpdatedAt().toString());

        printer.print(format, proj, out);
    }
}
