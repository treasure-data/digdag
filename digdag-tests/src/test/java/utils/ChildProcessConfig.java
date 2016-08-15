package utils;

import com.google.common.base.Optional;
import org.immutables.value.Value;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
public interface ChildProcessConfig
{
    Optional<Path> workdir();

    Class<?> mainClass();

    List<String> args();

    @Value.Default
    default boolean captureStdOut()
    {
        return true;
    }

    @Value.Default
    default boolean captureStdErr()
    {
        return true;
    }

    Map<String, String> environment();

    Map<String, String> properties();

    static ImmutableChildProcessConfig.Builder builder()
    {
        return ImmutableChildProcessConfig.builder();
    }
}
