package io.digdag.cli.client;

import java.lang.reflect.Field;
import java.util.Properties;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import io.digdag.client.DigdagClient;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ClientBuildingTest
{
    private String buildEndpoint(String endpoint)
        throws Exception
    {
        DigdagClient client = ClientCommand.buildClient(endpoint, ImmutableMap.of(), new Properties(), false, ImmutableMap.of(), ImmutableList.of());
        Field f = client.getClass().getDeclaredField("endpoint");
        f.setAccessible(true);
        return (String) f.get(client);
    }

    @Test
    public void endpoint() throws Exception
    {
        assertThat(buildEndpoint("127.0.0.1"), is("http://127.0.0.1:80"));
        assertThat(buildEndpoint("http://127.0.0.1"), is("http://127.0.0.1:80"));
        assertThat(buildEndpoint("https://127.0.0.1"), is("https://127.0.0.1:443"));
        assertThat(buildEndpoint("http://127.0.0.1:999"), is("http://127.0.0.1:999"));
    }
}
