package io.digdag.spi;

import java.util.List;

public interface OperatorProvider
{
    List<OperatorFactory> get();
}
