package io.digdag.spi;

import com.google.common.collect.ImmutableList;

import java.util.List;

public interface Operator
{
    TaskResult run(TaskExecutionContext ctx);

    /**
     * Get a list of secret selectors that describe the namespace(s) of secret keys that this
     * operator intends to access. An attempt to access a secret using a key not covered by
     * one of these selectors will result in a {@link SecretAccessDeniedException}.
     */
    default List<String> secretSelectors()
    {
        return ImmutableList.of();
    }
}
