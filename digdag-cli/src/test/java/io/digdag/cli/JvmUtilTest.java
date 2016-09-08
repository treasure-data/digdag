package io.digdag.cli;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Properties;

import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class JvmUtilTest
{
    @Mock CommandContext ctx;

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

    private void assertJdkVersionValidationNotPass(String v)
    {
        try {
            JvmUtil.validateJavaRuntime(newJdkProperties(v), ctx);
            fail();
        }
        catch (SystemExitException ex) {
        }
    }

    private void assertJdkVersionValidationPass(String v)
    {
        try {
            JvmUtil.validateJavaRuntime(newJdkProperties(v), ctx);
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
