package io.digdag.standards;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.treasuredata.client.ProxyConfig;
import org.junit.Test;

import java.net.URI;

import static io.digdag.standards.Proxies.proxyConfigFromEnv;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ProxiesTest
{
    private static final String PROXY_URL = "https://me:%27(%23%25@example.com:9119";

    @Test
    public void testNoProxyVar()
            throws Exception
    {
        assertThat(proxyConfigFromEnv("http", ImmutableMap.of()), is(Optional.absent()));
        assertThat(proxyConfigFromEnv("https", ImmutableMap.of()), is(Optional.absent()));

        assertThat(proxyConfigFromEnv("http", ImmutableMap.of("foo", "bar")), is(Optional.absent()));
        assertThat(proxyConfigFromEnv("https", ImmutableMap.of("foo", "bar")), is(Optional.absent()));
    }

    @Test
    public void testHttp()
            throws Exception
    {
        // Verify that http_proxy and HTTP_PROXY are used for http requests
        assertExpectedProxyConfig(proxyConfigFromEnv("http", ImmutableMap.of(
                "http_proxy", PROXY_URL,
                "HTTP_PROXY", "http://ignore",
                "https_proxy", "http://ignore",
                "HTTPS_PROXY", "http://ignore")).get());
        assertExpectedProxyConfig(proxyConfigFromEnv("http", ImmutableMap.of(
                "HTTP_PROXY", PROXY_URL,
                "https_proxy", "http://ignore",
                "HTTPS_PROXY", "http://ignore")).get());

        // Verify that https_proxy and HTTPS_PROXY are not used for http requests
        assertThat(proxyConfigFromEnv("http", ImmutableMap.of(
                "https_proxy", PROXY_URL,
                "HTTPS_PROXY", "http://ignore")), is(Optional.absent()));

        assertThat(proxyConfigFromEnv("http", ImmutableMap.of(
                "HTTPS_PROXY", PROXY_URL)), is(Optional.absent()));
    }

    @Test
    public void testHttps()
            throws Exception
    {
        assertExpectedProxyConfig(proxyConfigFromEnv("https", ImmutableMap.of(
                "https_proxy", PROXY_URL,
                "HTTPS_PROXY", "http://ignore",
                "http_proxy", "http://ignore",
                "HTTP_PROXY", "http://ignore")).get());

        assertExpectedProxyConfig(proxyConfigFromEnv("https", ImmutableMap.of(
                "HTTPS_PROXY", PROXY_URL,
                "http_proxy", "http://ignore",
                "HTTP_PROXY", "http://ignore")).get());

        assertExpectedProxyConfig(proxyConfigFromEnv("https", ImmutableMap.of(
                "http_proxy", PROXY_URL,
                "HTTP_PROXY", "http://ignore")).get());

        assertExpectedProxyConfig(proxyConfigFromEnv("https", ImmutableMap.of(
                "HTTP_PROXY", PROXY_URL)).get());
    }

    private void assertExpectedProxyConfig(ProxyConfig proxyConfig)
    {
        assertThat(proxyConfig.getUser(), is(Optional.of("me")));
        assertThat(proxyConfig.getPassword(), is(Optional.of("'(#%")));
        assertThat(proxyConfig.getUri(), is(URI.create("https://example.com:9119")));
    }
}
