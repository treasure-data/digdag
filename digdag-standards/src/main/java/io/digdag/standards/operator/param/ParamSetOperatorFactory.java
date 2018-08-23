package io.digdag.standards.operator.param;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;
import org.skife.jdbi.v2.DBI;

import java.util.List;

public class ParamSetOperatorFactory
        implements OperatorFactory
{
    private final Config systemConfig;
    private DBI dbi;

    @Inject
    public ParamSetOperatorFactory(Config systemConfig, @Named("param_server.database") DBI dbi)
    {
        this.systemConfig = systemConfig;
        this.dbi = dbi;
    }

    @Override
    public String getType()
    {
        return "param_set";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new ParamSetOperator(context, this.dbi);
    }

    private class ParamSetOperator
            extends BaseOperator
    {
        private DBI dbi;

        public ParamSetOperator(OperatorContext context, DBI dbi)
        {
            super(context);
            this.dbi = dbi;
        }

        @Override
        public TaskResult runTask()
        {
            Optional<String> paramServerType = systemConfig.getOptional("param_server.database.type", String.class);
            if(!paramServerType.isPresent()){
                throw new ConfigException("param_server.database.type is required to use this operator.");
            }

            ParamServerClient paramServerClient =
                ParamServer.getClient(
                        paramServerType.get(),
                        systemConfig,
                        this.dbi
                );

            Config params = request.getLocalConfig();

            List<String> keys = params.getKeys();
            if (keys.size() == 0) {
                throw new ConfigException("no key is set.");
            }

            for (String key : keys) {
                Optional<String> value = params.getOptional(key, String.class);
                if (value.isPresent()) {
                    paramServerClient.set(key, value.get(), request.getSiteId());
                }
            }

            paramServerClient.finalize();

            return TaskResult.empty(request);
        }
    }
}
