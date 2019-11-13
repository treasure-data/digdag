package io.digdag.standards.command.kubernetes;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.Charset;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.storage.StorageManager;

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

    private static KubernetesClientConfig createFromSystemConfig(final Optional<String> name,
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
        final Config extracted = validateParams(StorageManager.extractKeyPrefix(systemConfig, keyPrefix));
        if (extracted.get("use_kube_config", boolean.class, false)){
            io.fabric8.kubernetes.client.Config kubeConfig;
            kubeConfig = getKubeConfigFromPath(extracted.get("kube_config_path", String.class));
            return create(clusterName,
                kubeConfig.getMasterUrl(),
                kubeConfig.getCaCertData(),
                kubeConfig.getOauthToken(),
                kubeConfig.getNamespace()
              );
        }else{
            return create(clusterName,
                    extracted.get("master", String.class),
                    extracted.get("certs_ca_data", String.class),
                    extracted.get("oauth_token", String.class),
                    extracted.get("namespace", String.class));
        }
    }

    private static Config validateParams(final Config config)
    {
        if (!(config.has("master") &&
                config.has("certs_ca_data") &&
                config.has("oauth_token") &&
                config.has("namespace")) &&
                !(config.has("use_kube_config") &&
                 config.has("kube_config_path"))
            ) {
            throw new ConfigException("kubernetes config must have master:, certs_ca_data:, oauth_token: and namespace: Or use_kube_config: kube_config_path");
        }
        return config;
    }

    private static io.fabric8.kubernetes.client.Config getKubeConfigFromPath(String path)
    {
      try{
        final Path kubeConfigPath = Paths.get(path);
        final String kubeConfigContents = new String(Files.readAllBytes(kubeConfigPath), Charset.forName("UTF-8"));
        return io.fabric8.kubernetes.client.Config.fromKubeconfig(kubeConfigContents);
      }catch (java.io.IOException e) {
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
