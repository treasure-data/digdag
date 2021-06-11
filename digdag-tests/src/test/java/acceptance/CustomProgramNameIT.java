package acceptance;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.CommandStatus;
import utils.TestUtils;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CustomProgramNameIT
{
    @Before
    public void setUp()
            throws Exception
    {
        System.setProperty("io.digdag.cli.programName", "foo bar");
    }

    @After
    public void tearDown()
            throws Exception
    {
        System.clearProperty("io.digdag.cli.programName");
    }

    @Test
    public void testCustomProgramName()
            throws Exception
    {
        CommandStatus status = TestUtils.main("--help");
        assertThat(status.errUtf8(), status.code(), is(0));
        assertThat(status.errUtf8(), containsString("Usage: foo bar"));
    }
}
