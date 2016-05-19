package io.digdag.standards.operator.td;

import com.treasuredata.client.TDClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

class TDClientFactory
{
    static TDClient clientFromConfig(Config params)
    {

        String apikey = params.get("apikey", String.class).trim();
        if (apikey.isEmpty()) {
            throw new ConfigException("Parameter 'apikey' is empty");
        }

        TDClient client = TDClient.newBuilder(false)
                .setEndpoint(params.get("endpoint", String.class, "api.treasuredata.com"))
                .setUseSSL(params.get("use_ssl", boolean.class, true))
                .setApiKey(apikey)
                .setRetryLimit(0)  // disable td-client's retry mechanism
                .build();

        return client;
    }
}
