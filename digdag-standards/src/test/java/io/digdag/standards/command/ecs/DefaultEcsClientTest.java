package io.digdag.standards.command.ecs;

import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.Failure;
import com.amazonaws.services.ecs.model.ListTagsForResourceRequest;
import com.amazonaws.services.ecs.model.ListTagsForResourceResult;
import com.amazonaws.services.ecs.model.ListTaskDefinitionsRequest;
import com.amazonaws.services.ecs.model.ListTaskDefinitionsResult;
import com.amazonaws.services.ecs.model.RunTaskRequest;
import com.amazonaws.services.ecs.model.RunTaskResult;
import com.amazonaws.services.ecs.model.Tag;
import com.amazonaws.services.ecs.model.TaskDefinition;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.AWSLogsException;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
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
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
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
    @Mock
    private ListTaskDefinitionsResult listResult;
    @Mock
    private TaskDefinition taskDefinition;

    private DefaultEcsClient ecsClient;

    @Before
    public void setUp()
    {
        ecsClient = spy(new DefaultEcsClient(ecsClientConfig, rawEcsClient, logs, 10, 2, 1, 5, 1));
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
    public void testGetTaskDefinitionByTagsWithPredicateAndFindMatchedOne()
    {
        final List<Tag> expectedTags = ImmutableList.of(
                new Tag().withKey("k1").withValue("v1"),
                new Tag().withKey("k2").withValue("v2")
        );
        final String expectedTaskDefinitionArn = "my_task_def_arn";
        final ListTagsForResourceRequest request = new ListTagsForResourceRequest()
                .withResourceArn(expectedTaskDefinitionArn);
        final ListTagsForResourceResult result = new ListTagsForResourceResult()
                .withTags(expectedTags);
        doReturn(result).when(rawEcsClient).listTagsForResource(request);

        final ListTaskDefinitionsRequest listRequest = new ListTaskDefinitionsRequest();
        doReturn(listResult).when(rawEcsClient).listTaskDefinitions(listRequest);

        final List<String> taskDefinitionArns = Arrays.asList(expectedTaskDefinitionArn);
        doReturn(taskDefinitionArns).when(listResult).getTaskDefinitionArns();
        doReturn(taskDefinition).when(ecsClient).getTaskDefinition(expectedTaskDefinitionArn);

        // This condition will match with expected one.
        final Predicate<List<Tag>> condition = tags -> tags.stream().allMatch(t -> t.getKey().startsWith("k"));
        final Optional<TaskDefinition> actual = ecsClient.getTaskDefinitionByTags(condition);
        assertTrue(actual.isPresent());
        assertEquals(taskDefinition, actual.get());
    }

    @Test
    public void testGetTaskDefinitionByTagsWithPredicateAndDidNotFindMatchedOne()
    {
        final List<Tag> expectedTags = ImmutableList.of(
                new Tag().withKey("k1").withValue("v1"),
                new Tag().withKey("k2").withValue("v2")
        );
        final String expectedTaskDefinitionArn = "my_task_def_arn";
        final ListTagsForResourceRequest request = new ListTagsForResourceRequest()
                .withResourceArn(expectedTaskDefinitionArn);
        final ListTagsForResourceResult result = new ListTagsForResourceResult()
                .withTags(expectedTags);
        doReturn(result).when(rawEcsClient).listTagsForResource(request);

        final ListTaskDefinitionsRequest listRequest = new ListTaskDefinitionsRequest();
        doReturn(listResult).when(rawEcsClient).listTaskDefinitions(listRequest);

        final List<String> taskDefinitionArns = Arrays.asList(expectedTaskDefinitionArn);
        doReturn(taskDefinitionArns).when(listResult).getTaskDefinitionArns();

        // This condition will not match with anything.
        final Predicate<List<Tag>> condition = tags -> tags.stream().allMatch(t -> t.getKey().startsWith("l"));
        final Optional<TaskDefinition> actual = ecsClient.getTaskDefinitionByTags(condition);
        assertFalse(actual.isPresent());
    }

    @Test
    public void testSubmitTask()
    {
        final RunTaskResult expectedResult = mock(RunTaskResult.class);
        doReturn(expectedResult).when(rawEcsClient).runTask(any());

        final RunTaskRequest request = mock(RunTaskRequest.class);
        final RunTaskResult actualResult = ecsClient.submitTask(request);

        assertThat(actualResult, is(expectedResult));
        Mockito.verify(rawEcsClient, times(1)).runTask(any());
        Mockito.verify(ecsClient, times(0)).waitWithRandomJitter(anyLong(), anyLong());
    }

    @Test
    public void testSumbitTaskRetryOnceOnAgentError()
    {
        final RunTaskResult failureResult = mock(RunTaskResult.class);
        final Failure failure = mock(Failure.class);
        doReturn("AGENT").when(failure).getReason();
        doReturn(Arrays.asList(failure)).when(failureResult).getFailures();

        final RunTaskResult expectedResult = mock(RunTaskResult.class);

        when(rawEcsClient.runTask(any()))
            .thenReturn(failureResult)
            .thenReturn(expectedResult);

        final RunTaskRequest request = mock(RunTaskRequest.class);
        final RunTaskResult actualResult = ecsClient.submitTask(request);

        assertThat(actualResult, is(expectedResult));
        Mockito.verify(rawEcsClient, times(2)).runTask(any());
        Mockito.verify(ecsClient, times(1)).waitWithRandomJitter(anyLong(), anyLong());
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
                }
                else {
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
