package io.digdag.core.log;

public enum LogLevel
{
    ERROR(40),
    WARN(30),
    INFO(20),
    DEBUG(10),
    TRACE(0);

    private final int level;

    private LogLevel(int level)
    {
        this.level = level;
    }

    public int toInt()
    {
        return level;
    }

    public static LogLevel of(String name)
    {
        switch (name) {
        case "ERROR":
        case "error":
            return ERROR;
        case "WARN":
        case "warn":
            return WARN;
        case "INFO":
        case "info":
            return INFO;
        case "DEBUG":
        case "debug":
            return DEBUG;
        case "TRACE":
        case "trace":
            return TRACE;
        default:
            throw new IllegalArgumentException("Unknown log level: " + name);
        }
    }
}
