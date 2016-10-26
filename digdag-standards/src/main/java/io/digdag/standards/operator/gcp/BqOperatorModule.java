package io.digdag.standards.operator.gcp;

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
        binder.bind(BqClient.Factory.class);
        binder.bind(GcpCredentialProvider.class);
        addStandardOperatorFactory(binder, BqOperatorFactory.class);
        addStandardOperatorFactory(binder, BqLoadOperatorFactory.class);
        addStandardOperatorFactory(binder, BqExtractOperatorFactory.class);
        addStandardOperatorFactory(binder, BqDdlOperatorFactory.class);
    }

    protected void addStandardOperatorFactory(Binder binder, Class<? extends OperatorFactory> factory)
    {
        Multibinder.newSetBinder(binder, OperatorFactory.class)
                .addBinding().to(factory).in(Scopes.SINGLETON);
    }
}
