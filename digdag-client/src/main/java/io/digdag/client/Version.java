package io.digdag.client;

import com.google.common.base.Throwables;
import com.google.common.io.Resources;

import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Version
{
    public static final String VERSION_PROPERTY = Version.class.getName() + ".version";

    private final String version;

    private Version(String version)
    {
        this.version = version;
    }

    public static Version buildVersion()
    {
        return Version.of(versionString());
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
            throw Throwables.propagate(e);
        }
    }

    public String version()
    {
        return version;
    }

    public static Version of(String versionString)
    {
        return new Version(versionString);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Version version1 = (Version) o;

        return version != null ? version.equals(version1.version) : version1.version == null;
    }

    @Override
    public int hashCode()
    {
        return version != null ? version.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return version;
    }
}
