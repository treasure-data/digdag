package io.digdag.cli;

import com.beust.jcommander.ParameterException;
import org.junit.Test;

public class BasicAuthParameterValidatorTest
{
    @Test
    public void validUserPassPasses()
    {
        new BasicAuthParameterValidator().validate(null, "user:pass");
    }

    @Test(expected = ParameterException.class)
    public void missingUsernameFails()
    {
        new BasicAuthParameterValidator().validate(null, ":pass");
    }

    @Test(expected = ParameterException.class)
    public void missingPasswordFails()
    {
        new BasicAuthParameterValidator().validate(null, "user:");
    }

    @Test(expected = ParameterException.class)
    public void missingColonFails()
    {
        new BasicAuthParameterValidator().validate(null, "userpass");
    }
}