package io.digdag.cli;

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
        assertFalse(Main.isValidJavaVersion("1.7.0"));
        assertFalse(Main.isValidJavaVersion("1.8.0_40"));

        assertTrue(Main.isValidJavaVersion("1.8.0_73"));
        assertTrue(Main.isValidJavaVersion("1.8.0_74"));
    }
}