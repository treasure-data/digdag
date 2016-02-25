package io.digdag.cli;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.classic.Level;
import io.digdag.core.log.LogLevel;
import io.digdag.core.log.TaskContextLogging;
import io.digdag.core.log.TaskContextLogging.Context;
import static ch.qos.logback.classic.Level.ERROR_INT;
import static ch.qos.logback.classic.Level.WARN_INT;
import static ch.qos.logback.classic.Level.INFO_INT;
import static ch.qos.logback.classic.Level.DEBUG_INT;
import static ch.qos.logback.classic.Level.TRACE_INT;

public class LogbackTaskContextLoggerBridgeAppender
    extends UnsynchronizedAppenderBase<ILoggingEvent>
{
    private static final String PATTERN = "%d{yyyy-MM-dd HH:mm:ss.SSS Z} [%level] (%thread\\) %class: %m%n";

    private PatternLayout layout;

    @Override
    public void start()
    {
        if (isStarted()) {
            return;
        }

        PatternLayout patternLayout = new PatternLayout();
        patternLayout.setContext(context);
        patternLayout.setPattern(PATTERN);
        patternLayout.setOutputPatternAsHeader(false);
        patternLayout.start();
        this.layout = patternLayout;

        super.start();
    }

    @Override
    protected void append(ILoggingEvent event)
    {
        Context ctx = TaskContextLogging.getContext();
        if (ctx == null) {
            return;
        }
        LogLevel level = logLevel(event.getLevel());
        if (!ctx.matches(level)) {
            return;
        }
        String message = layout.doLayout(event);
        ctx.getLogger().log(level, event.getTimeStamp(), message);
    }

    private static LogLevel logLevel(Level level)
    {
        int lv = level.toInt();
        if (lv >= ERROR_INT) {
            return LogLevel.ERROR;
        }
        else if (lv >= WARN_INT) {
            return LogLevel.WARN;
        }
        else if (lv >= INFO_INT) {
            return LogLevel.INFO;
        }
        else if (lv >= DEBUG_INT) {
            return LogLevel.DEBUG;
        }
        else {
            return LogLevel.TRACE;
        }
    }
}
