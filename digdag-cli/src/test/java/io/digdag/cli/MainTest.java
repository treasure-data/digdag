package io.digdag.cli;

import java.util.Properties;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 */
public class MainTest
{
    @Test
    public void shouldCheckJavaVersion()
    {
        assertJdkVersionValidationNotPass("1.7.0");
        assertJdkVersionValidationNotPass("1.8.0_40");
        assertJdkVersionValidationNotPass("1.8.0_70");
        assertJdkVersionValidationPass("1.8.0_71");
        assertJdkVersionValidationPass("1.9.0");
        assertJdkVersionValidationPass("1.9.0_1");
    }

    private static void assertJdkVersionValidationNotPass(String v)
    {
        try {
            Main.validateJavaRuntime(newJdkProperties(v));
            fail();
        }
        catch (SystemExitException ex) {
        }
    }

    private static void assertJdkVersionValidationPass(String v)
    {
        try {
            Main.validateJavaRuntime(newJdkProperties(v));
        }
        catch (SystemExitException ex) {
            fail();
        }
    }

    private static Properties newJdkProperties(String version)
    {
        Properties props = new Properties();
        props.setProperty("java.vm.name", "OpenJDK");
        props.setProperty("java.version", version);
        return props;
    }
}
