package io.digdag.standards.command.kubernetes;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandRequest;
import static io.digdag.client.config.ConfigUtils.newConfig;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.NodeAffinityBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.Container;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import io.digdag.spi.TaskRequest;
import io.digdag.client.config.Config;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static io.digdag.core.workflow.OperatorTestingUtils.newContext;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

@RunWith(MockitoJUnitRunner.class)
public class DefaultKubernetesClientTest
{

    private KubernetesClientConfig kubernetesClientConfig;
    private io.fabric8.kubernetes.client.DefaultKubernetesClient k8sDefaultKubernetesClient;
    private CommandContext commandContext;
    private CommandRequest commandRequest;

    @Before
    public void setUp()
            throws Exception
    {
        kubernetesClientConfig = mock(KubernetesClientConfig.class);
        k8sDefaultKubernetesClient =  mock(io.fabric8.kubernetes.client.DefaultKubernetesClient.class);
        commandContext = mock(CommandContext.class);
        commandRequest = mock(CommandRequest.class);
    }

    @Test
    public void testCreateContainer()
            throws Exception
    {
        final Config taskRequestConfig = newConfig()
                .set("kubernetes", newConfig().set(
                        "container", newConfig().set(
                                "volumeMounts", ImmutableList.of(
                                      newConfig().set("mountPath", "/test-ebs").set("name", "test")))))
                .set("docker", newConfig().set("image", "test"));

        final TaskRequest taskRequest = newTaskRequest().withConfig(taskRequestConfig);
        when(commandContext.getTaskRequest()).thenReturn(taskRequest);
        DefaultKubernetesClient defaultKubernetesClient = new DefaultKubernetesClient(kubernetesClientConfig, k8sDefaultKubernetesClient);

        String podName = "test";
        List<String> commands = new ArrayList<>();
        List<String> arguments = new ArrayList<>();
        Container container = defaultKubernetesClient.createContainer(commandContext, commandRequest, podName, commands, arguments);

        Container desiredContainer = new ContainerBuilder()
            .withName(podName)
            .withImage("test")
            .withCommand(commands)
            .withArgs(arguments)
            .withResources(defaultKubernetesClient.toResourceRequirements(defaultKubernetesClient.getResourceLimits(commandContext, commandRequest), defaultKubernetesClient.getResourceRequests(commandContext, commandRequest)))
            .withVolumeMounts(Arrays.asList(new VolumeMountBuilder().withName("test").withMountPath("/test-ebs").build())).build();

        assertThat(container, is(desiredContainer));
    }

    @Test
    public void testCreatePodSPec()
            throws Exception
    {
        final Config taskRequestConfig = newConfig()
                .set("kubernetes", newConfig().set(
                        "spec", newConfig().set(
                                "affinity", newConfig().set(
                                        "nodeAffinity", newConfig().set(
                                                "requiredDuringSchedulingIgnoredDuringExecution", newConfig().set(
                                                        "nodeSelectorTerms", ImmutableList.of(
                                                                newConfig().set(
                                                                        "matchExpressions", ImmutableList.of(
                                                                                newConfig().set("key", "failure-domain.beta.kubernetes.io/zone")
                                                                                .set("operator", "In")
                                                                                .set("values", ImmutableList.of(
                                                                                                "asia-northeast1-a"
                                )))))))))));

        final TaskRequest taskRequest = newTaskRequest().withConfig(taskRequestConfig);
        when(commandContext.getTaskRequest()).thenReturn(taskRequest);
        DefaultKubernetesClient defaultKubernetesClient = new DefaultKubernetesClient(kubernetesClientConfig, k8sDefaultKubernetesClient);

        Container container = mock(Container.class);
        PodSpec podSpec = defaultKubernetesClient.createPodSpec(commandContext, commandRequest, container);

        PodSpec desiredPodSpec = new PodSpecBuilder()
            .addToContainers(container)
            .withRestartPolicy("Never")
            .withAffinity(new AffinityBuilder()
                .withNodeAffinity(new NodeAffinityBuilder()
                  .withRequiredDuringSchedulingIgnoredDuringExecution(new NodeSelectorBuilder()
                    .addToNodeSelectorTerms(0,
                      new NodeSelectorTermBuilder().addToMatchExpressions(0,
                        new NodeSelectorRequirementBuilder()
                          .withKey("failure-domain.beta.kubernetes.io/zone")
                          .addToValues(0,"asia-northeast1-a")
                          .withOperator("In")
                          .build()
                        )
                      .build()
                      )
                    .build()
                    )
                  .build()
                  )
                .build()
                )
            .build();

        assertThat(podSpec, is(desiredPodSpec));
    }
}
