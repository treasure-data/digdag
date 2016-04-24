package io.digdag.cli;

import java.io.PrintStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class JvmUtil
{
    static void validateJavaRuntime(Properties systemProps, PrintStream err)
        throws SystemExitException
    {
        String javaVmName = systemProps.getProperty("java.vm.name", "");
        if (javaVmName.startsWith("OpenJDK") || javaVmName.startsWith("Java HotSpot")) {
            // OpenJDK: OpenJDK 64-Bit Server VM
            // Oracle JDK: Java HotSpot(TM) 64-Bit Server VM
            // Android Dalvik: Dalvik
            // Kaffe: Kaffe
            String javaVersion = systemProps.getProperty("java.version", "");
            validateOpenJdkVersion(javaVersion, err);
        }
        else {
            err.println("Unsupported java.vm.name (" + javaVmName + "). Digdag may not work. Please OpenJDK instead.");
        }
    }

    private static void validateOpenJdkVersion(String javaVersion, PrintStream err)
        throws SystemExitException
    {
        Matcher m = Pattern.compile("^(\\d+)\\.(\\d+)\\.(\\d+)(?:_(\\d+))?").matcher(javaVersion);
        if (m.find()) {
            // OpenJDK: 1.8.0_73
            int major = Integer.parseInt(m.group(1));
            int minor = Integer.parseInt(m.group(2));
            int revision = Integer.parseInt(m.group(3));
            int update = m.group(4) == null ? 0 : Integer.parseInt(m.group(4));
            if (major < 1) {
                throw openJdkVersionCheckError(javaVersion);
            }
            else if (major == 1) {
                if (minor < 8) {
                    throw openJdkVersionCheckError(javaVersion);
                }
                else if (minor == 8) {
                    if (revision < 0) {
                        throw openJdkVersionCheckError(javaVersion);
                    }
                    else if (revision == 0) {
                        if (update < 71) {
                            throw openJdkVersionCheckError(javaVersion);
                        }
                    }
                }
            }
        }
        else {
            err.println("Unsupported java version syntax (" + javaVersion + "). Digdag may not work. Please use OpenJDK instead.");
        }
    }

    private static SystemExitException openJdkVersionCheckError(String javaVersion)
    {
        return SystemExitException.systemExit("Found too old java version (" + javaVersion + "). Please use at least JDK 8u71 (1.8.0_71).");
    }

    public static void validateJavaRuntime(PrintStream err)
            throws SystemExitException
    {
        validateJavaRuntime(System.getProperties(), err);
    }
}
