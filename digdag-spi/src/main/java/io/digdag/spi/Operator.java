package io.digdag.spi;

import com.google.common.collect.ImmutableList;

import java.util.List;

public interface Operator
{
    TaskResult run();
}
