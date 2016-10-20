package io.digdag.standards.operator;

import org.immutables.value.Value;

import java.time.Duration;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
public interface DurationInterval
{
    Duration min();
    Duration max();

    static DurationInterval of(Duration min, Duration max)
    {
        return ImmutableDurationInterval.builder()
                .min(min)
                .max(max)
                .build();
    }
}
