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

    public int getCode()
    {
        return code;
    }
}
