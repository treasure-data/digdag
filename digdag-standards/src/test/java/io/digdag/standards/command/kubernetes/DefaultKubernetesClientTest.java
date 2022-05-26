package io.digdag.standards.command.kubernetes;

import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandRequest;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyListOf;

@RunWith(MockitoJUnitRunner.class)
public class DefaultKubernetesClientTest
{
    @Mock private KubernetesClientConfig config;
    @Mock private io.fabric8.kubernetes.client.DefaultKubernetesClient client;
    @Mock private CommandContext context;
    @Mock private CommandRequest request;
    @Mock private MixedOperation<io.fabric8.kubernetes.api.model.Pod, PodList, PodResource<io.fabric8.kubernetes.api.model.Pod>> pods;
    @Mock private NonNamespaceOperation<io.fabric8.kubernetes.api.model.Pod, PodList, PodResource<io.fabric8.kubernetes.api.model.Pod>> namespace;
    @Mock private io.fabric8.kubernetes.api.model.Pod fabricPod;
    @Mock private ObjectMeta objectMeta;
    @Mock private Container container;

    @Test
    public void testRunPod()
            throws Exception
    {
        String podName = "testPod";
        doReturn(objectMeta).when(fabricPod).getMetadata();
        doReturn(podName).when(objectMeta).getName();
        doReturn(pods).when(client).pods();
        doReturn(namespace).when(pods).inNamespace(anyString());
        doReturn(fabricPod).when(namespace).create(any(io.fabric8.kubernetes.api.model.Pod.class));

        final DefaultKubernetesClient kubernetesClient = spy(new DefaultKubernetesClient(config, client));
        doReturn(container).when(kubernetesClient).createContainer(any(CommandContext.class), any(CommandRequest.class), anyString(), anyListOf(String.class), anyListOf(String.class));

        List<String> commands =  new ArrayList<String>();
        List<String> arguments =  new ArrayList<String>();
        Pod pod = kubernetesClient.runPod(context, request, podName, commands, arguments);
        assertThat(pod.getName(), is(podName));
    }
}
