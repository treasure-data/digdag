package io.digdag.standards.command.kubernetes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.storage.StorageManager;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class KubernetesClientConfig
{
    private static final String KUBERNETES_CLIENT_PARAMS_PREFIX = "agent.command_executor.kubernetes.";

    public static KubernetesClientConfig create(final Optional<String> name,
            final Config systemConfig,
            final Config requestConfig)
    {
        if (requestConfig.has("kubernetes")) {
            // from task request config
            return KubernetesClientConfig.createFromTaskRequestConfig(name, requestConfig.getNested("kubernetes"));
        }
        else {
            // from system config
            return KubernetesClientConfig.createFromSystemConfig(name, systemConfig);
        }
    }

    private static KubernetesClientConfig createFromTaskRequestConfig(final Optional<String> name,
            final Config config)
    {
        // TODO
        // We'd better to customize cluster config by task request config??
        throw new UnsupportedOperationException("Not support yet");
    }

    @VisibleForTesting
    static KubernetesClientConfig createFromSystemConfig(final Optional<String> name,
            final io.digdag.client.config.Config systemConfig)
    {
        final String clusterName;
        if (!name.isPresent()) {
            if (!systemConfig.get("agent.command_executor.type", String.class, "").equals("kubernetes")) {
                throw new ConfigException("agent.command_executor.type: is not 'kubernetes'");
            }
            clusterName = systemConfig.get(KUBERNETES_CLIENT_PARAMS_PREFIX + "name", String.class);
        }
        else {
            clusterName = name.get();
        }
        final String keyPrefix = KUBERNETES_CLIENT_PARAMS_PREFIX + clusterName + ".";
        final Config extracted = StorageManager.extractKeyPrefix(systemConfig, keyPrefix);
        if (extracted.has("kube_config_path")) {
            io.fabric8.kubernetes.client.Config kubeConfig;
            kubeConfig = getKubeConfigFromPath(extracted.get("kube_config_path", String.class));
            return create(clusterName,
                    kubeConfig.getMasterUrl(),
                    kubeConfig.getCaCertData(),
                    kubeConfig.getOauthToken(),
                    kubeConfig.getNamespace());
        } else {
            final Config validated = validateParams(extracted);
            return create(clusterName,
                    validated.get("master", String.class),
                    validated.get("certs_ca_data", String.class),
                    validated.get("oauth_token", String.class),
                    validated.get("namespace", String.class));
        }
    }

    private static Config validateParams(final Config config)
    {
        if (!config.has("master") ||
                !config.has("certs_ca_data") ||
                !config.has("oauth_token") ||
                !config.has("namespace")) {
            throw new ConfigException("kubernetes config must have master:, certs_ca_data:, oauth_token: and namespace: or kube_config_path:");
        }
        return config;
    }

    @VisibleForTesting
    static io.fabric8.kubernetes.client.Config getKubeConfigFromPath(String path)
    {
      try {
          final Path kubeConfigPath = Paths.get(path);
          final String kubeConfigContents = new String(Files.readAllBytes(kubeConfigPath), Charset.forName("UTF-8"));
          return io.fabric8.kubernetes.client.Config.fromKubeconfig(kubeConfigContents);
      } catch (java.io.IOException e) {
          throw new ConfigException("Could not read kubeConfig, check out kube_config_path.");
      }
    }

    private static KubernetesClientConfig create(final String name,
            final String master,
            final String certsCaData,
            final String oauthToken,
            final String namespace)
    {
        System.setProperty("kubernetes.auth.tryKubeConfig", "false");
        return new KubernetesClientConfig(name, master, namespace, certsCaData, oauthToken);
    }

    private final String name;
    private final String master;
    private final String namespace;
    private final String certsCaData;
    private final String oauthToken;

    private KubernetesClientConfig(final String name,
            final String master,
            final String namespace,
            final String certsCaData,
            final String oauthToken)
    {
        this.name = name;
        this.master = master;
        this.namespace = namespace;
        this.certsCaData = certsCaData;
        this.oauthToken = oauthToken;
    }

    public String getName()
    {
        return this.name;
    }

    public String getMaster()
    {
        return this.master;
    }

    public String getNamespace()
    {
        return this.namespace;
    }

    public String getCertsCaData()
    {
        return this.certsCaData;
    }

    public String getOauthToken()
    {
        return this.oauthToken;
    }
}
