package io.digdag.cli;

public class SystemExitException
        extends Exception
{
    private final int code;

    public SystemExitException(int code, String message)
    {
        super(message);
        this.code = code;
    }

    public static SystemExitException systemExit(String errorMessage)
    {
        if (errorMessage != null) {
            return new SystemExitException(1, errorMessage);
        }
        else {
            return new SystemExitException(0, null);
        }
    }

    public int getCode()
    {
        return code;
    }
}
