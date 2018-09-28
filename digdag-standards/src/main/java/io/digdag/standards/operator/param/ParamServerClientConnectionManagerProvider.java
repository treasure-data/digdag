package io.digdag.standards.operator.param;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.ParamServerClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;

public class ParamServerClientConnectionManagerProvider
        implements Provider<ParamServerClientConnectionManager>
{
    Config systemConfig;
    ParamServerClientConnectionManager client;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Inject
    public ParamServerClientConnectionManagerProvider(Config systemConfig)
    {
        this.systemConfig = systemConfig;
        String databaseType = systemConfig.get("param_server.type", String.class, "");
        logger.debug("Using param_server database type: {}", databaseType);

        switch(databaseType){
            case "postgresql":
                client = new PostgresqlServerClientConnectionManager(systemConfig);
                break;
            case "redis":
                client = new RedisServerClientConnectionManager(systemConfig);
                break;
            case "":
                client = new DummyServerClientConnectionManager();
                break;
            default:
                throw new ConfigException("Unsupported param_server.type : " + databaseType);
        }
    }

    @Override
    public ParamServerClientConnectionManager get()
    {
        return client;
    }

    @PreDestroy
    public synchronized void shutdown(){
        logger.debug("shutdown called");
        client.shutdown();
    }
}
