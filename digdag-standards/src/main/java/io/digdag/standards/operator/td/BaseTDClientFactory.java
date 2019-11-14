package io.digdag.standards.operator.td;

import com.treasuredata.client.TDClient;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretProvider;

import java.util.Map;

public interface BaseTDClientFactory
{
    TDClient createClient(TDOperator.SystemDefaultConfig systemDefaultConfig, Map<String, String> env, Config params, SecretProvider secrets);
}
