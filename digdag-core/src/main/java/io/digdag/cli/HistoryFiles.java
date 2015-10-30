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

    public File getSessionDir(TimeZone timeZone, Date scheduleTime)
    {
        String name = new SimpleDateFormat("yyyy-MM-dd_HHmmss").format(scheduleTime);
        return new File(dir, name);
    }
}
