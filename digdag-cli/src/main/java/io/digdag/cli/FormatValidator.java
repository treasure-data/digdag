package io.digdag.cli;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;

public class FormatValidator
        implements IValueValidator
{
    @Override
    public void validate(String name, Object value)
            throws ParameterException
    {
        try {
            OutputFormat.valueOf(String.valueOf(value));
        }
        catch (IllegalArgumentException e) {
            throw new ParameterException("invalid format: " + value);
        }
    }
}
