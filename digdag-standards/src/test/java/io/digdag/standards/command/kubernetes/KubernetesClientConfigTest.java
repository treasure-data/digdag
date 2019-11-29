package io.digdag.standards.command.kubernetes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.standards.command.kubernetes.KubernetesClientConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesClientConfigTest
{
    private final ObjectMapper om = new ObjectMapper();
    private final ConfigFactory cf = new ConfigFactory(om);

    private static final String KUBERNETES_CLIENT_PARAMS_PREFIX = "agent.command_executor.kubernetes.";
    private final Optional<String> clusterName = Optional.of("test");
    private String kubeConfigPath;

    @Before
    public void setUp()
            throws Exception
    {
        File file = new File("src/test/resources/io/digdag/standards/command/kubernetes/kube_config.yaml");
        kubeConfigPath = file.getAbsolutePath();
    }

    @Test
    public void testKubeConfigFromPath()
            throws Exception
    {
        io.fabric8.kubernetes.client.Config kubeConfig = KubernetesClientConfig.getKubeConfigFromPath(kubeConfigPath);

        String masterUrl = "https://127.0.0.1";
        String namespace = "default";
        String caCertData = "test=";
        String oauthToken = "test=";
        assertThat(masterUrl, is(kubeConfig.getMasterUrl()));
        assertThat(caCertData, is(kubeConfig.getCaCertData()));
        assertThat(oauthToken, is(kubeConfig.getOauthToken()));
        assertThat(namespace, is(kubeConfig.getNamespace()));
    }

    @Test
    public void testCreateFromSystemConfig()
            throws Exception
    {
        final Config systemConfig = cf.create()
          .set("agent.command_executor.type", "kubernetes")
          .set(KUBERNETES_CLIENT_PARAMS_PREFIX+"test.master", "https://127.0.0.1")
          .set(KUBERNETES_CLIENT_PARAMS_PREFIX+"test.certs_ca_data", "test=")
          .set(KUBERNETES_CLIENT_PARAMS_PREFIX+"test.oauth_token", "test=")
          .set(KUBERNETES_CLIENT_PARAMS_PREFIX+"test.namespace", "default");

        KubernetesClientConfig kubernetesClientConfig = KubernetesClientConfig.createFromSystemConfig(clusterName, systemConfig);

        String masterUrl = "https://127.0.0.1";
        String namespace = "default";
        String caCertData = "test=";
        String oauthToken = "test=";
        assertThat(masterUrl, is(kubernetesClientConfig.getMaster()));
        assertThat(caCertData, is(kubernetesClientConfig.getCertsCaData()));
        assertThat(oauthToken, is(kubernetesClientConfig.getOauthToken()));
        assertThat(namespace, is(kubernetesClientConfig.getNamespace()));
    }

    @Test
    public void testCreateFromSystemConfigWithKubeConfig()
            throws Exception
    {
        final Config systemConfig = cf.create()
          .set("agent.command_executor.type", "kubernetes")
          .set(KUBERNETES_CLIENT_PARAMS_PREFIX+"test.kube_config_path", kubeConfigPath);

        KubernetesClientConfig kubernetesClientConfig = KubernetesClientConfig.createFromSystemConfig(clusterName, systemConfig);

        String masterUrl = "https://127.0.0.1";
        String namespace = "default";
        String caCertData = "test=";
        String oauthToken = "test=";
        assertThat(masterUrl, is(kubernetesClientConfig.getMaster()));
        assertThat(caCertData, is(kubernetesClientConfig.getCertsCaData()));
        assertThat(oauthToken, is(kubernetesClientConfig.getOauthToken()));
        assertThat(namespace, is(kubernetesClientConfig.getNamespace()));
    }
}
