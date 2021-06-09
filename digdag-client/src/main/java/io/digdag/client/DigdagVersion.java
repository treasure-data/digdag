package io.digdag.client;

import com.google.common.io.Resources;
import io.digdag.commons.guava.ThrowablesUtil;

import java.io.IOException;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DigdagVersion
{
    public static final String VERSION_PROPERTY = Version.class.getName() + ".version";

    public static Version buildVersion()
    {
        return Version.parse(versionString());
    }

    private static String versionString()
    {
        // First read version from system property
        String propertyVersion = System.getProperty(VERSION_PROPERTY);
        if (propertyVersion != null) {
            return propertyVersion;
        }

        // Then read version file
        try {
            return Resources.toString(Resources.getResource(Version.class, "version.txt"), UTF_8).trim();
        }
        catch (IOException e) {
            throw ThrowablesUtil.propagate(e);
        }
    }
}
