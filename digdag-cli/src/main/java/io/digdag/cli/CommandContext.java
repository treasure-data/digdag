package io.digdag.cli;

import org.immutables.value.Value;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.Map;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
public interface CommandContext
{
    Map<String, String> environment();

    io.digdag.core.Version version();

    String programName();

    InputStream in();

    PrintStream out();

    PrintStream err();

    static ImmutableCommandContext.Builder builder()
    {
        return ImmutableCommandContext.builder();
    }
}
