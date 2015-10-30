package io.digdag.cli;

import java.io.File;
import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import io.digdag.core.*;

public class HistoryFiles
{
    private final File dir;

    public HistoryFiles(File dir)
    {
        this.dir = dir;
    }

    // returns dir/workflowName/yyyy-MM-dd-HHmmss
    public File getSessionDir(WorkflowSource wf, TimeZone timeZone, Date scheduleTime)
    {
        String name = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(scheduleTime);
        return new File(new File(dir, wf.getName()), name);
    }
}
