package io.digdag.standards.command.kubernetes;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TemporalConfigStorageTest
{
    private final ObjectMapper om = new ObjectMapper();
    private final ConfigFactory cf = new ConfigFactory(om);
    private static final String TEMPORAL_CONFIG_STORAGE_PARAMS_PREFIX = "agent.command_executor.kubernetes.config_storage.";

    @Test(expected = ConfigException.class)
    public void failValidateSystemConfig()
    {
        final Config systemConfig = cf.create();
        TemporalConfigStorage.validateSystemConfig("in", systemConfig);
    }

    @Test
    public void successValidateSystemConfig()
    {
        final Config systemConfig = cf.create()
            .set(TEMPORAL_CONFIG_STORAGE_PARAMS_PREFIX+"in.type", "s3")
            .set(TEMPORAL_CONFIG_STORAGE_PARAMS_PREFIX+"in.s3.bucket", "test")
            .set(TEMPORAL_CONFIG_STORAGE_PARAMS_PREFIX+"out.type", "s3")
            .set(TEMPORAL_CONFIG_STORAGE_PARAMS_PREFIX+"out.s3.bucket", "test");

        TemporalConfigStorage.validateSystemConfig("in", systemConfig);
        TemporalConfigStorage.validateSystemConfig("out", systemConfig);
    }
}
