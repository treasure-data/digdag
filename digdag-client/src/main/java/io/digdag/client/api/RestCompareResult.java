package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestCompareResult.class)
public interface RestCompareResult
{
    String getRaw();

    // {
    //   files: [
    //     {
    //       from: "a",
    //       to: "b",
    //       diff: [
    //         {
    //           from: {
    //             line: "a",
    //             column: "b",
    //             text: "...",
    //           },
    //           to: {
    //             line: "...",
    //             column: "...",
    //             text: "...",
    //           }
    //         }
    //       ]
    //     },
    //     {
    //     }
    //   ]
    // }

    static ImmutableRestCompareResult.Builder builder()
    {
        return ImmutableRestCompareResult.builder();
    }
}
