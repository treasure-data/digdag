package io.digdag.cli.client;

import java.lang.reflect.Field;

import org.junit.Test;

import io.digdag.client.DigdagClient;

import static io.digdag.core.Version.buildVersion;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ClientBuildingTest
{
    private String buildEndpoint(String endpoint)
        throws Exception
    {
        ShowWorkflow cmd = new ShowWorkflow(buildVersion(), System.out, System.err);
        cmd.endpoint = endpoint;
        DigdagClient client = cmd.buildClient(false);
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
