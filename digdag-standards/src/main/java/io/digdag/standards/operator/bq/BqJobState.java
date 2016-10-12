package io.digdag.standards.operator.bq;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(visibility = Value.Style.ImplementationVisibility.PACKAGE)
@JsonSerialize(as = ImmutableBqJobState.class)
@JsonDeserialize(as = ImmutableBqJobState.class)
interface BqJobState
{
    Optional<String> jobId();

    BqJobState withJobId(String value);
    BqJobState withJobId(Optional<String> value);

    static BqJobState empty()
    {
        return ImmutableBqJobState.builder().build();
    }
}
