package io.digdag.standards.command.kubernetes;

import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.TaskRequest;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.NodeAffinityBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirementBuilder;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static io.digdag.core.workflow.OperatorTestingUtils.newTaskRequest;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultKubernetesClientTest
{

    private KubernetesClientConfig kubernetesClientConfig;
    private io.fabric8.kubernetes.client.DefaultKubernetesClient k8sDefaultKubernetesClient;
    private CommandContext commandContext;
    private CommandRequest commandRequest;
    private Config testKubernetesConfig;

    @Before
    public void setUp()
            throws Exception
    {
        kubernetesClientConfig = mock(KubernetesClientConfig.class);
        k8sDefaultKubernetesClient =  mock(io.fabric8.kubernetes.client.DefaultKubernetesClient.class);
        commandContext = mock(CommandContext.class);
        commandRequest = mock(CommandRequest.class);

        /*
            kubernetes:
              Pod:
                volumeMounts:
                - mountPath: "/test-ebs"
                  name: "test"
                - mountPath: "/test-ebs"
                  name: "test2"
                volumes:
                - name: test
                  emptyDir: {}
                - name: test2
                  emptyDir: {}
                resources:
                  limits:
                    memory: 200Mi
                  requests:
                    memory: 100Mi
                affinity:
                  nodeAffinity:
                    requiredDuringSchedulingIgnoredDuringExecution
                      nodeSelectorTerms:
                      - matchExpressions
                        - key: test
                          operator: In
                          values:
                          - test
                tolerations:
                - key: test
                  operator: Exists
                  effect: NoSchedule
                - key: test2
                  operator: Exists
                  effect: NoSchedule
              PersistentVolumeClaim:
                accessModes:
                - ReadWriteOnce
                volumeMode: Block
                resources:
                  requests:
                    storage: 10Gi
              PersistentVolume:
                capacity:
                  storage: 10Gi
                accessModes:
                - ReadWriteOnce
                volumeMode: "Block"
                persistentVolumeReclaimPolicy: "ReadWriteOnce"
                fc:
                  targetWWNs: ["50060e801049cfd1"]
                  lun: 0
                  readOnly: false
        */
        testKubernetesConfig = newConfig()
                .set("Pod", newConfig()
                        .set("volumeMounts", ImmutableList.of(
                                newConfig().set("mountPath", "/test-ebs").set("name", "test"),
                                newConfig().set("mountPath", "/test-ebs2").set("name", "test2")))
                        .set("volumes", ImmutableList.of(
                                newConfig().set("name", "test").set("emptyDir", newConfig()),
                                newConfig().set("name", "test2").set("emptyDir", newConfig())))
                        .set("resources", newConfig()
                                .set("limits", newConfig().set("memory", "200Mi"))
                                .set("requests", newConfig().set("memory", "100Mi")))
                        .set("affinity", newConfig()
                                .set("nodeAffinity", newConfig()
                                        .set("requiredDuringSchedulingIgnoredDuringExecution", newConfig()
                                                .set("nodeSelectorTerms", ImmutableList.of(
                                                        newConfig().set("matchExpressions", ImmutableList.of(
                                                                newConfig().set("key", "test1").set("operator", "In").set("values", ImmutableList.of("test1")))))))))
                        .set("tolerations", ImmutableList.of(
                                newConfig().set("key", "test").set("operator", "Exists").set("effect", "NoSchedule"),
                                newConfig().set("key", "test2").set("operator", "Exists").set("effect", "NoSchedule"))))
                .set("PersistentVolumeClaim", newConfig()
                        .set("accessModes", ImmutableList.of("ReadWriteOnce"))
                        .set("volumeMode", "Block")
                        .set("resources", newConfig().set("requests", newConfig().set("storage", "10Gi"))))
                .set("PersistentVolume", newConfig()
                        .set("capacity", newConfig().set("storage", "10Gi"))
                        .set("accessModes", ImmutableList.of("ReadWriteOnce"))
                        .set("volumeMode", "Block")
                        .set("persistentVolumeReclaimPolicy", "Retain")
                        .set("fc", newConfig()
                                .set("targetWWNs", ImmutableList.of("50060e801049cfd1"))
                                .set("lun", "0")
                                .set("readOnly", "false")));
    }

    @Test
    public void testCreateContainer()
            throws Exception
    {
        final Config taskRequestConfig = newConfig().set("docker", newConfig().set("image", "test"));

        final TaskRequest taskRequest = newTaskRequest().withConfig(taskRequestConfig);
        when(commandContext.getTaskRequest()).thenReturn(taskRequest);

        String podName = "test";
        List<String> commands = new ArrayList<>();
        List<String> arguments = new ArrayList<>();

        DefaultKubernetesClient defaultKubernetesClient = new DefaultKubernetesClient(kubernetesClientConfig, k8sDefaultKubernetesClient);
        Container container = defaultKubernetesClient.createContainer(commandContext, commandRequest, null, podName, commands, arguments);

        Container desiredContainer = new ContainerBuilder()
                .withName(podName)
                .withImage("test")
                .withCommand(commands)
                .withArgs(arguments)
                .withResources(null)
                .withVolumeMounts((List<VolumeMount>) null)
                .build();

        assertThat(container, is(desiredContainer));
    }

    @Test
    public void testCreateContainerWithKubernetesConfig()
            throws Exception
    {
        final Config kubernetesPodConfig = testKubernetesConfig.get("Pod", Config.class);
        final Config taskRequestConfig = newConfig()
                .set("kubernetes", testKubernetesConfig)
                .set("docker", newConfig().set("image", "test"));

        final TaskRequest taskRequest = newTaskRequest().withConfig(taskRequestConfig);
        when(commandContext.getTaskRequest()).thenReturn(taskRequest);

        String podName = "test";
        List<String> commands = new ArrayList<>();
        List<String> arguments = new ArrayList<>();

        DefaultKubernetesClient defaultKubernetesClient = new DefaultKubernetesClient(kubernetesClientConfig, k8sDefaultKubernetesClient);
        Container container = defaultKubernetesClient.createContainer(commandContext, commandRequest, kubernetesPodConfig, podName, commands, arguments);

        Container desiredContainer = new ContainerBuilder()
                .withName(podName)
                .withImage("test")
                .withCommand(commands)
                .withArgs(arguments)
                .withResources(new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("200Mi"))
                        .addToRequests("memory", new Quantity("100Mi"))
                        .build())
                .withVolumeMounts(Arrays.asList(
                        new VolumeMountBuilder().withName("test").withMountPath("/test-ebs").build(),
                        new VolumeMountBuilder().withName("test2").withMountPath("/test-ebs2").build()))
                .build();

        assertThat(container, is(desiredContainer));
    }

    @Test
    public void testCreatePodSPec()
            throws Exception
    {
        final Config taskRequestConfig = newConfig().set("docker", newConfig().set("image", "test"));

        final TaskRequest taskRequest = newTaskRequest().withConfig(taskRequestConfig);
        when(commandContext.getTaskRequest()).thenReturn(taskRequest);
        Container container = mock(Container.class);

        DefaultKubernetesClient defaultKubernetesClient = new DefaultKubernetesClient(kubernetesClientConfig, k8sDefaultKubernetesClient);
        PodSpec podSpec = defaultKubernetesClient.createPodSpec(commandContext, commandRequest, null, container);

        PodSpec desiredPodSpec = new PodSpecBuilder()
            .addToContainers(container)
            .withRestartPolicy("Never")
            .withAffinity(null)
            .withTolerations((List<Toleration>)null)
            .withVolumes((List<Volume>)null)
            .build();

        assertThat(podSpec, is(desiredPodSpec));
    }

    @Test
    public void testCreatePodSPecWithKubernetesConfig()
            throws Exception
    {
        final Config kubernetesPodConfig = testKubernetesConfig.get("Pod", Config.class);
        final Config taskRequestConfig = newConfig()
                .set("kubernetes", testKubernetesConfig)
                .set("docker", newConfig().set("image", "test"));

        final TaskRequest taskRequest = newTaskRequest().withConfig(taskRequestConfig);
        when(commandContext.getTaskRequest()).thenReturn(taskRequest);
        Container container = mock(Container.class);

        DefaultKubernetesClient defaultKubernetesClient = new DefaultKubernetesClient(kubernetesClientConfig, k8sDefaultKubernetesClient);
        PodSpec podSpec = defaultKubernetesClient.createPodSpec(commandContext, commandRequest, kubernetesPodConfig, container);

        PodSpec desiredPodSpec = new PodSpecBuilder()
            .addToContainers(container)
            .withRestartPolicy("Never")
            .withAffinity(new AffinityBuilder()
                .withNodeAffinity(new NodeAffinityBuilder()
                        .withRequiredDuringSchedulingIgnoredDuringExecution(new NodeSelectorBuilder()
                                .addToNodeSelectorTerms(0,
                                        new NodeSelectorTermBuilder()
                                                .addToMatchExpressions(0, new NodeSelectorRequirementBuilder()
                                                        .withKey("test1")
                                                        .addToValues(0, "test1")
                                                        .withOperator("In")
                                                        .build()
                                                ).build()
                                ).build()
                        ).build()
                ).build())
                .withTolerations(Arrays.asList(
                        new TolerationBuilder().withKey("test").withOperator("Exists").withEffect("NoSchedule").build(),
                        new TolerationBuilder().withKey("test2").withOperator("Exists").withEffect("NoSchedule").build()))
                .withVolumes(Arrays.asList(
                        new VolumeBuilder().withName("test").withEmptyDir(new EmptyDirVolumeSource()).build(),
                        new VolumeBuilder().withName("test2").withEmptyDir(new EmptyDirVolumeSource()).build()))
            .build();

        assertThat(podSpec, is(desiredPodSpec));
    }
}
