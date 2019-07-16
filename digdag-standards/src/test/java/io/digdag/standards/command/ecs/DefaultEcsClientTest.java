package io.digdag.standards.command.ecs;

import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.ListTagsForResourceRequest;
import com.amazonaws.services.ecs.model.ListTagsForResourceResult;
import com.amazonaws.services.ecs.model.Tag;
import com.amazonaws.services.logs.AWSLogs;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class DefaultEcsClientTest
{
    @Mock private EcsClientConfig ecsClientConfig;
    @Mock private AmazonECSClient rawEcsClient;
    @Mock private AWSLogs logs;

    private DefaultEcsClient ecsClient;

    @Before
    public void setUp()
    {
        ecsClient = spy(new DefaultEcsClient(ecsClientConfig, rawEcsClient, logs));
    }

    @Test
    public void testGetTaskDefinitionTags()
            throws Exception
    {
        final List<Tag> expected = ImmutableList.of(
                new Tag().withKey("k1").withValue("v1"),
                new Tag().withKey("k2").withValue("v2")
        );
        final ListTagsForResourceRequest request = new ListTagsForResourceRequest()
                .withResourceArn("my_task_def_arn");
        final ListTagsForResourceResult result = new ListTagsForResourceResult()
                .withTags(expected);
        doReturn(result).when(rawEcsClient).listTagsForResource(request);

        List<Tag> tags = ecsClient.getTaskDefinitionTags(request.getResourceArn());
        assertEquals(expected.size(), tags.size());
        assertEquals(expected.get(0), tags.get(0));
        assertEquals(expected.get(1), tags.get(1));
    }
}
