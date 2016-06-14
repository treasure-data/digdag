package io.digdag.standards.operator;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.spi.OperatorFactory;
import io.digdag.standards.operator.IfOperatorFactory;
import io.digdag.standards.operator.FailOperatorFactory;
import io.digdag.standards.operator.EchoOperatorFactory;
import io.digdag.standards.operator.NopOperatorFactory;
import io.digdag.standards.operator.PyOperatorFactory;
import io.digdag.standards.operator.RbOperatorFactory;
import io.digdag.standards.operator.ShOperatorFactory;
import io.digdag.standards.operator.MailOperatorFactory;
import io.digdag.standards.operator.LoopOperatorFactory;
import io.digdag.standards.operator.ForEachOperatorFactory;
import io.digdag.standards.operator.EmbulkOperatorFactory;
import io.digdag.standards.operator.td.TdForEachOperatorFactory;
import io.digdag.standards.operator.td.TdOperatorFactory;
import io.digdag.standards.operator.td.TdRunOperatorFactory;
import io.digdag.standards.operator.td.TdLoadOperatorFactory;
import io.digdag.standards.operator.td.TdDdlOperatorFactory;
import io.digdag.standards.operator.td.TdTableExportOperatorFactory;
import io.digdag.standards.operator.td.TdWaitOperatorFactory;
import io.digdag.standards.operator.td.TdWaitTableOperatorFactory;

public class OperatorModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        addStandardOperatorFactory(binder, PyOperatorFactory.class);
        addStandardOperatorFactory(binder, RbOperatorFactory.class);
        addStandardOperatorFactory(binder, ShOperatorFactory.class);
        addStandardOperatorFactory(binder, MailOperatorFactory.class);
        addStandardOperatorFactory(binder, LoopOperatorFactory.class);
        addStandardOperatorFactory(binder, ForEachOperatorFactory.class);
        addStandardOperatorFactory(binder, EmbulkOperatorFactory.class);
        addStandardOperatorFactory(binder, TdOperatorFactory.class);
        addStandardOperatorFactory(binder, TdForEachOperatorFactory.class);
        addStandardOperatorFactory(binder, TdRunOperatorFactory.class);
        addStandardOperatorFactory(binder, TdLoadOperatorFactory.class);
        addStandardOperatorFactory(binder, TdDdlOperatorFactory.class);
        addStandardOperatorFactory(binder, TdTableExportOperatorFactory.class);
        addStandardOperatorFactory(binder, TdWaitOperatorFactory.class);
        addStandardOperatorFactory(binder, TdWaitTableOperatorFactory.class);
        addStandardOperatorFactory(binder, EchoOperatorFactory.class);
        addStandardOperatorFactory(binder, IfOperatorFactory.class);
        addStandardOperatorFactory(binder, FailOperatorFactory.class);
        addStandardOperatorFactory(binder, NotifyOperatorFactory.class);
    }

    protected void addStandardOperatorFactory(Binder binder, Class<? extends OperatorFactory> factory)
    {
        Multibinder.newSetBinder(binder, OperatorFactory.class)
            .addBinding().to(factory).in(Scopes.SINGLETON);
    }
}
