package io.digdag.core;

import java.util.List;
import java.util.Date;

public interface ScheduleStarter
{
    void start(int workflowId, Date scheduleTime);
}
