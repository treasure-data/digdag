package io.digdag.spi;

public interface OperatorFactory
{
    String getType();

    Operator newOperator(OperatorContext context);
}
