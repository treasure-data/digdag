package io.digdag.core;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.io.Resources;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Version
{
    private static final Supplier<String> VERSION = Suppliers.memoize(Version::loadVersion);

    private static String loadVersion()
    {
        try {
            return Resources.toString(Resources.getResource(Version.class, "version.txt"), UTF_8).trim();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static String version()
    {
        return VERSION.get();
    }
}
