package io.digdag.standards.command.kubernetes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.storage.StorageManager;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.TaskRequest;
import io.digdag.standards.command.kubernetes.KubernetesClientConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.lang.reflect.Method;

import java.lang.ClassLoader;
import java.io.File;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.is;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesClientConfigTest
{
    private final ObjectMapper om = new ObjectMapper();
    private final ConfigFactory cf = new ConfigFactory(om);

    private static final String KUBERNETES_CLIENT_PARAMS_PREFIX = "agent.command_executor.kubernetes.";
    private String kubeConfigPath;
    private final Optional<String> clusterName = Optional.of("test");

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

        final Config systemConfig = cf.create()
          .set("agent.command_executor.type", "kubernetes")
          .set(KUBERNETES_CLIENT_PARAMS_PREFIX+"test.kube_config_path", kubeConfigPath);
        final Config requestConfig = cf.create();

        KubernetesClientConfig kubernetesClientConfig = KubernetesClientConfig.create(clusterName, systemConfig, requestConfig);

        Method method = KubernetesClientConfig.class.getDeclaredMethod("getKubeConfigFromPath", String.class);
        method.setAccessible(true);

        io.fabric8.kubernetes.client.Config kubeConfig = (io.fabric8.kubernetes.client.Config)method.invoke(kubernetesClientConfig, kubeConfigPath);

        String masterUrl = "https://127.0.0.1";
        String namespace = "default";
        String test = "test=";
        assertThat(masterUrl, is(kubeConfig.getMasterUrl()));
        assertThat(test, is(kubeConfig.getCaCertData()));
        assertThat(test, is(kubeConfig.getOauthToken()));
        assertThat(namespace, is(kubeConfig.getNamespace()));
    }
}
