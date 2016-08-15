package utils;

import com.google.common.base.Joiner;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertThat;

public class ChildProcessFactoryTest
{
    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private ChildProcessFactory factory;

    @Before
    public void setUp()
            throws Exception
    {
        factory = new ChildProcessFactory();
    }

    @Test
    public void testSpawn()
            throws Exception
    {
        ChildProcess childProcess = factory.spawn(Foobar.class, "foo", "bar");
        int code = childProcess.waitFor();
        assertThat(childProcess.errUtf8(), code, Matchers.is(0));
        assertThat(childProcess.outUtf8(), Matchers.containsString("Hello world: foo bar"));
    }

    public static class Foobar
    {
        public static void main(String[] args)
        {
            System.out.println("Hello world: " + Joiner.on(' ').join(args));
        }
    }
}