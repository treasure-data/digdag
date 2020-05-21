package io.digdag.standards.command.ecs;

import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.ListTagsForResourceRequest;
import com.amazonaws.services.ecs.model.ListTagsForResourceResult;
import com.amazonaws.services.ecs.model.Tag;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.AWSLogsException;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultEcsClientTest
{
    @Mock
    private EcsClientConfig ecsClientConfig;
    @Mock
    private AmazonECSClient rawEcsClient;
    @Mock
    private AWSLogs logs;

    private DefaultEcsClient ecsClient;

    @Before
    public void setUp()
    {
        ecsClient = spy(new DefaultEcsClient(ecsClientConfig, rawEcsClient, logs, 10, 2, 1));
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

    @Test
    public void testRetryOnRateLimit()
    {
        Supplier<Boolean> func = new Supplier<Boolean>()
        {
            private final int maxError = 3;
            private int current = 0;

            @Override
            public Boolean get()
            {
                current += 1;
                if (current <= maxError) {
                    AWSLogsException ex = new AWSLogsException("Rate exceeded");
                    ex.setErrorCode("ThrottlingException");
                    ex.setStatusCode(400);
                    ex.setServiceName("AWSLogs");
                    ex.setRequestId("testRetryOnRateLimit");
                    throw ex;
                }
                return true;
            }
        };
        assertEquals(true, ecsClient.retryOnRateLimit(func));
        Mockito.verify(ecsClient, times(3)).waitWithRandomJitter(anyLong(), anyLong());
    }

    @Test
    public void testGetLog()
    {
        final List<Boolean> answerList = new ArrayList<>();
        // Throw exception 3times then succeed.
        Answer<GetLogEventsResult> answer = new Answer<GetLogEventsResult>()
        {
            int count = 0;

            public GetLogEventsResult answer(InvocationOnMock invocation)
            {
                count++;
                if (count > 3) {
                    answerList.add(Boolean.TRUE);
                    return new GetLogEventsResult();
                } else {
                    AWSLogsException ex = new AWSLogsException("Rate exceeded");
                    ex.setErrorCode("ThrottlingException");
                    ex.setStatusCode(400);
                    ex.setServiceName("AWSLogs");
                    ex.setRequestId("testGetLog");
                    answerList.add(Boolean.FALSE);
                    throw ex;
                }
            }
        };
        when(ecsClient.getLog("dummyGroupName", "dummyStreamName", Optional.absent())).then(answer);

        ecsClient.getLog("dummyGroupName", "dummyStreamName", Optional.absent());
        assertThat(answerList.size(), is(4));
        assertThat(answerList.get(3), is(true));
    }
}
