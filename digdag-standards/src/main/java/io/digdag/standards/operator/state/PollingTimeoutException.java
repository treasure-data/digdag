package io.digdag.standards.operator.state;

public class PollingTimeoutException extends RuntimeException
{
    public PollingTimeoutException(String message)
    {
        super(message);
    }
}
