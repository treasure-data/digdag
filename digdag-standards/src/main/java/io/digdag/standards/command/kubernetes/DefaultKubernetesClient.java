package io.digdag.standards.command.kubernetes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.digdag.client.config.Config;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandRequest;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.dsl.PodResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

public class DefaultKubernetesClient
        implements KubernetesClient
{
    private static Logger logger = LoggerFactory.getLogger(DefaultKubernetesClient.class);

    private final KubernetesClientConfig config;
    private final io.fabric8.kubernetes.client.DefaultKubernetesClient client;

    public DefaultKubernetesClient(final KubernetesClientConfig config,
            final io.fabric8.kubernetes.client.DefaultKubernetesClient client)
    {
        this.config = config;
        this.client = client;
    }

    @Override
    public KubernetesClientConfig getConfig()
    {
        return config;
    }

    @Override
    public Pod runPod(final CommandContext context, final CommandRequest request,
            final String name, final List<String> commands, final List<String> arguments)
    {
        final Container container = createContainer(context, request, name, commands, arguments);
        final PodSpec podSpec = createPodSpec(context, request, container);
        final io.fabric8.kubernetes.api.model.Pod pod = client.pods()
                .inNamespace(client.getNamespace())
                .createNew()
                .withNewMetadata()
                .withName(name)
                .withLabels(getPodLabels())
                .endMetadata()
                .withSpec(podSpec)
                .done();
        return Pod.of(pod);
    }

    @Override
    public Pod pollPod(final String podName)
    {
        final io.fabric8.kubernetes.api.model.Pod pod = client.pods()
                .inNamespace(client.getNamespace())
                .withName(podName)
                .get();
        return Pod.of(pod);
    }

    @Override
    public boolean deletePod(final String podName)
    {
        // TODO need to retry?

        // TODO
        // We'd better to consider about pods graceful deletion here.
        //
        // References:
        //   https://kubernetes.io/docs/concepts/workloads/pods/pod/#termination-of-pods
        //   https://kubernetes.io/docs/tasks/run-application/force-delete-stateful-set-pod/
        return client.pods()
                .inNamespace(client.getNamespace())
                .withName(podName)
                .delete();
    }

    @Override
    public boolean isWaitingContainerCreation(final Pod pod)
    {
        // TODO
        // We assume that a single container running on a pod. If we will use multiples containers on a pod,
        // the logic should be changed.
        final ContainerStatus containerStatus = pod.getStatus().getContainerStatuses().get(0);
        return containerStatus.getState().getWaiting() != null;
    }

    @Override
    public String getLog(final String podName, final long offset)
            throws IOException
    {
        final PodResource podResource = client.pods().withName(podName);
        final Reader reader = podResource.getLogReader(); // return InputStreamReader
        try {
            reader.skip(offset); // skip the chars that were already read
            return CharStreams.toString(reader); // TODO not use String object
        }
        finally {
            reader.close();
        }
    }

    protected Map<String, String> getPodLabels()
    {
        return ImmutableMap.of();
    }

    protected Container createContainer(final CommandContext context, final CommandRequest request,
            final String name, final List<String> commands, final List<String> arguments)
    {
        return new ContainerBuilder()
                .withName(name)
                .withImage(getContainerImage(context, request))
                .withEnv(toEnvVars(getEnvironments(context, request)))
                .withResources(toResourceRequirements(getResourceLimits(context, request), getResourceRequests(context, request)))
                .withCommand(commands)
                .withArgs(arguments)
                .build();
    }

    protected PodSpec createPodSpec(final CommandContext context, final CommandRequest request,
            final Container container)
    {
        // TODO
        // Revisit what values should be extracted as config params or system config params

        return new PodSpecBuilder()
                //.withHostNetwork(true);
                //.withDnsPolicy("ClusterFirstWithHostNet");
                .addToContainers(container)
                // TODO extract as config parameter
                // Restart policy is "Never" by default since it needs to avoid executing the operator multiple times. It might not
                // make the script idempotent.
                // https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-policy
                .withRestartPolicy("Never")
                .build();
    }

    protected String getContainerImage(final CommandContext context, final CommandRequest request)
    {
        final Config config = context.getTaskRequest().getConfig();
        final Config dockerConfig = config.getNested("docker");
        return dockerConfig.get("image", String.class);
    }

    protected Map<String, String> getEnvironments(final CommandContext context, final CommandRequest request)
    {
        return request.getEnvironments();
    }

    protected Map<String, String> getResourceLimits(final CommandContext context, final CommandRequest request)
    {
        return ImmutableMap.of();
    }

    protected Map<String, String> getResourceRequests(final CommandContext context, final CommandRequest request)
    {
        return ImmutableMap.of();
    }

    private static List<EnvVar> toEnvVars(final Map<String, String> environments)
    {
        final ImmutableList.Builder<EnvVar> envVars = ImmutableList.builder();
        for (final Map.Entry<String, String> e : environments.entrySet()) {
            final EnvVar envVar = new EnvVarBuilder().withName(e.getKey()).withValue(e.getValue()).build();
            envVars.add(envVar);
        }
        return envVars.build();
    }

    private static ResourceRequirements toResourceRequirements(
            final Map<String, String> limits,
            final Map<String, String> requests)
    {
        final ImmutableMap.Builder<String, Quantity> ls = new ImmutableMap.Builder<>();
        for (Map.Entry<String, String> e : limits.entrySet()) {
            ls.put(e.getKey(), new Quantity(e.getValue()));
        }

        final ImmutableMap.Builder<String, Quantity> rs = new ImmutableMap.Builder<>();
        for (Map.Entry<String, String> e : requests.entrySet()) {
            rs.put(e.getKey(), new Quantity(e.getValue()));
        }

        return new ResourceRequirements(ls.build(), rs.build());
    }

    @Override
    public void close()
    {
        if (client != null) {
            client.close();
        }
    }
}
