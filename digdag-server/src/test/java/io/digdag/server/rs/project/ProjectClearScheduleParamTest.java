package io.digdag.server.rs.project;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.digdag.commons.AssertUtil.assertException;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

@RunWith(MockitoJUnitRunner.class)
public class ProjectClearScheduleParamTest {
    private final ProjectClearScheduleParam paramData1 = new ProjectClearScheduleParam(Arrays.asList("wf1", "wf2", "wf3"), false);
    private final ProjectClearScheduleParam paramData2 = new ProjectClearScheduleParam(Arrays.asList("wf1", "wf2"), true);

    @Test
    public void testCheckClear()
    {
        {
            assertThat(paramData1.checkClear("wf1"), is(true));
        }
        {
            assertThat(paramData1.checkClear("wf0"), is(false));
        }
        {
            assertThat(paramData2.checkClear("wf1"), is(true));
        }
        {
            assertThat(paramData2.checkClear("wf0"), is(true));
        }
    }

    @Test
    public void testCheckClearList()
    {
        {
            List<String> l1 = Arrays.asList("wf1", "wf2", "wf3", "wf4");
            Map<String, Boolean> result = paramData1.checkClearList(l1);
            assertThat(result, hasEntry("wf1", true));
            assertThat(result, hasEntry("wf2", true));
            assertThat(result, hasEntry("wf3", true));
            assertThat(result, hasEntry("wf4", false));
        }
        {
            List<String> l1 = Arrays.asList("wf1", "wf2", "wf3", "wf4");
            Map<String, Boolean> result = paramData2.checkClearList(l1);
            assertThat(result, hasEntry("wf1", true));
            assertThat(result, hasEntry("wf2", true));
            assertThat(result, hasEntry("wf3", true));
            assertThat(result, hasEntry("wf4", true));
        }
    }

    @Test
    public void testValidateWorkflowNames()
    {
        {
            List<String> allWorkflowNames = Arrays.asList("wf1", "wf2", "wf3", "wf4");
            paramData1.validateWorkflowNames(allWorkflowNames);
        }
        {
            List<String> allWorkflowNames = Arrays.asList("wf1", "wf2", "wf3");
            paramData1.validateWorkflowNames(allWorkflowNames);
        }
        {
            List<String> allWorkflowNames = Arrays.asList("wf1", "wf2", "wf4", "wf5");
            assertException(()->paramData1.validateWorkflowNames(allWorkflowNames), IllegalArgumentException.class, "Including non existent workflow wf3 must fail");
        }
        {
            List<String> allWorkflowNames = Arrays.asList();
            assertException(()->paramData1.validateWorkflowNames(allWorkflowNames), IllegalArgumentException.class, "Including non existent workflow must fail");
        }
        {
            List<String> allWorkflowNames = Arrays.asList("wf1", "wf3", "wf4", "wf5");
            assertException(()->paramData2.validateWorkflowNames(allWorkflowNames), IllegalArgumentException.class, "Including non existent workflow wf2 must fail");
        }
        {
            List<String> allWorkflowNames = Arrays.asList();
            assertException(()->paramData2.validateWorkflowNames(allWorkflowNames), IllegalArgumentException.class, "Including non existent workflow must fail");
        }
    }
}
