package io.digdag.standards.operator.bq;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.spi.OperatorFactory;

public class BqOperatorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(BqJobRunner.Factory.class);
        addStandardOperatorFactory(binder, BqOperatorFactory.class);
    }

    protected void addStandardOperatorFactory(Binder binder, Class<? extends OperatorFactory> factory)
    {
        Multibinder.newSetBinder(binder, OperatorFactory.class)
                .addBinding().to(factory).in(Scopes.SINGLETON);
    }
}
