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

    Optional<Integer> pollIteration();

    Optional<Integer> retryIteration();

    Optional<Boolean> started();

    BqJobState withJobId(String value);
    BqJobState withJobId(Optional<String> value);
    BqJobState withPollIteration(int value);
    BqJobState withPollIteration(Optional<Integer> value);
    BqJobState withRetryIteration(int value);
    BqJobState withRetryIteration(Optional<Integer> value);
    BqJobState withStarted(boolean value);
    BqJobState withStarted(Optional<Boolean> value);

    static BqJobState empty()
    {
        return ImmutableBqJobState.builder().build();
    }
}
