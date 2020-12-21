package io.digdag.cli;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

import java.util.regex.Pattern;

public class BasicAuthParameterValidator
        implements IParameterValidator
{
    private final static Pattern pattern = Pattern.compile(".+:.+");

    public void validate(String name, String value) throws ParameterException
    {
        if (!pattern.matcher(value).matches()) {
            throw new ParameterException("Expected format: username:password");
        }
    }
}