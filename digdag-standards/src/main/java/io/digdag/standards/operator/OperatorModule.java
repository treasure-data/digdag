package io.digdag.standards.operator;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.spi.OperatorFactory;
import io.digdag.standards.operator.aws.EmrOperatorFactory;
import io.digdag.standards.operator.aws.S3WaitOperatorFactory;
import io.digdag.standards.operator.param.ParamGetOperatorFactory;
import io.digdag.standards.operator.param.ParamSetOperatorFactory;
import io.digdag.standards.operator.pg.PgOperatorFactory;
import io.digdag.standards.operator.redshift.RedshiftLoadOperatorFactory;
import io.digdag.standards.operator.redshift.RedshiftOperatorFactory;
import io.digdag.standards.operator.redshift.RedshiftUnloadOperatorFactory;
import io.digdag.standards.operator.td.TDResultExportOperatorFactory;
import io.digdag.standards.operator.td.TdDdlOperatorFactory;
import io.digdag.standards.operator.td.TdForEachOperatorFactory;
import io.digdag.standards.operator.td.TdLoadOperatorFactory;
import io.digdag.standards.operator.td.TdOperatorFactory;
import io.digdag.standards.operator.td.TdPartialDeleteOperatorFactory;
import io.digdag.standards.operator.td.TdRunOperatorFactory;
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
        addStandardOperatorFactory(binder, ForRangeOperatorFactory.class);
        addStandardOperatorFactory(binder, EmbulkOperatorFactory.class);
        addStandardOperatorFactory(binder, TdOperatorFactory.class);
        addStandardOperatorFactory(binder, TdForEachOperatorFactory.class);
        addStandardOperatorFactory(binder, TdRunOperatorFactory.class);
        addStandardOperatorFactory(binder, TdLoadOperatorFactory.class);
        addStandardOperatorFactory(binder, TdDdlOperatorFactory.class);
        addStandardOperatorFactory(binder, TdTableExportOperatorFactory.class);
        addStandardOperatorFactory(binder, TdWaitOperatorFactory.class);
        addStandardOperatorFactory(binder, TdWaitTableOperatorFactory.class);
        addStandardOperatorFactory(binder, TdPartialDeleteOperatorFactory.class);
        addStandardOperatorFactory(binder, TDResultExportOperatorFactory.class);
        addStandardOperatorFactory(binder, EchoOperatorFactory.class);
        addStandardOperatorFactory(binder, IfOperatorFactory.class);
        addStandardOperatorFactory(binder, FailOperatorFactory.class);
        addStandardOperatorFactory(binder, WaitOperatorFactory.class);
        addStandardOperatorFactory(binder, NotifyOperatorFactory.class);
        addStandardOperatorFactory(binder, PgOperatorFactory.class);
        addStandardOperatorFactory(binder, RedshiftOperatorFactory.class);
        addStandardOperatorFactory(binder, RedshiftLoadOperatorFactory.class);
        addStandardOperatorFactory(binder, RedshiftUnloadOperatorFactory.class);
        addStandardOperatorFactory(binder, S3WaitOperatorFactory.class);
        addStandardOperatorFactory(binder, EmrOperatorFactory.class);
        addStandardOperatorFactory(binder, HttpOperatorFactory.class);
        addStandardOperatorFactory(binder, HttpCallOperatorFactory.class);
        addStandardOperatorFactory(binder, ParamSetOperatorFactory.class);
        addStandardOperatorFactory(binder, ParamGetOperatorFactory.class);
    }

    protected void addStandardOperatorFactory(Binder binder, Class<? extends OperatorFactory> factory)
    {
        Multibinder.newSetBinder(binder, OperatorFactory.class)
                .addBinding().to(factory).in(Scopes.SINGLETON);
    }
}
