package acceptance;

import org.junit.Rule;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableList;
import utils.TestUtils;

import static utils.TestUtils.copyResource;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RunWithResumeIT
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void runAll()
            throws IOException
    {
        copyResource("acceptance/resume/resume.dig", root().resolve("resume.dig"));
        run("--rerun");
        for (String f : new String[] {
            "seq1", "seq2", "seq3",
            "par1", "par2", "par3",
            "last"
        }) {
            Files.delete(root().resolve(f + ".out"));
        }
    }

    private void run(String... additional)
    {
        TestUtils.main(ImmutableList.copyOf(
                Iterables.concat(
                    ImmutableList.of("run", "-o", root().toString(), "--project", root().toString(), "resume.dig", "--session", "2016-01-01 00:00:00"),
                    ImmutableList.copyOf(additional))
                ).toArray(new String[0]));
    }

    private Path root()
    {
        return folder.getRoot().toPath().toAbsolutePath();
    }

    private void assertOutputExists(String name, boolean exists)
    {
        assertThat(Files.exists(root().resolve(name + ".out")), is(exists));
    }

    @Test
    public void testStartSeq1()
            throws Exception
    {
        run("--start", "+seq1");
        assertOutputExists("seq1", true);
        assertOutputExists("seq2", true);
        assertOutputExists("seq3", true);
        assertOutputExists("par1", true);
        assertOutputExists("par2", true);
        assertOutputExists("par3", true);
        assertOutputExists("last", true);
    }

    @Test
    public void testStartSeq2()
            throws Exception
    {
        run("--start", "+seq2");
        assertOutputExists("seq1", false);
        assertOutputExists("seq2", true);
        assertOutputExists("seq3", true);
        assertOutputExists("par1", true);
        assertOutputExists("par2", true);
        assertOutputExists("par3", true);
        assertOutputExists("last", true);
    }

    @Test
    public void testStartSeq2Nested()
            throws Exception
    {
        run("--start", "+seq2+nest");
        assertOutputExists("seq1", false);
        assertOutputExists("seq2", true);
        assertOutputExists("seq3", true);
        assertOutputExists("par1", true);
        assertOutputExists("par2", true);
        assertOutputExists("par3", true);
        assertOutputExists("last", true);
    }

    @Test
    public void testStartSeq3()
            throws Exception
    {
        run("--start", "+seq3");
        assertOutputExists("seq1", false);
        assertOutputExists("seq2", false);
        assertOutputExists("seq3", true);
        assertOutputExists("par1", true);
        assertOutputExists("par2", true);
        assertOutputExists("par3", true);
        assertOutputExists("last", true);
    }

    /*
    @Test
    public void testStartPar1()
            throws Exception
    {
        run("--start", "+par1");
        assertOutputExists("seq1", false);
        assertOutputExists("seq2", false);
        assertOutputExists("seq3", false);
        assertOutputExists("par1", true);
        //assertOutputExists("par2", false);
        //assertOutputExists("par3", false);
        assertOutputExists("last", true);
    }

    @Test
    public void testStartPar2()
            throws Exception
    {
        run("--start", "+par2");
        assertOutputExists("seq1", false);
        assertOutputExists("seq2", false);
        assertOutputExists("seq3", false);
        //assertOutputExists("par1", false);
        assertOutputExists("par2", true);
        //assertOutputExists("par3", false);
        assertOutputExists("last", true);
    }

    @Test
    public void testStartPar2Nested()
            throws Exception
    {
        run("--start", "+par2+nest");
        assertOutputExists("seq1", false);
        assertOutputExists("seq2", false);
        assertOutputExists("seq3", false);
        //assertOutputExists("par1", false);
        assertOutputExists("par2", true);
        //assertOutputExists("par3", false);
        assertOutputExists("last", true);
    }

    @Test
    public void testStartPar3()
            throws Exception
    {
        run("--start", "+par3");
        assertOutputExists("seq1", false);
        assertOutputExists("seq2", false);
        assertOutputExists("seq3", false);
        //assertOutputExists("par1", false);
        //assertOutputExists("par2", false);
        assertOutputExists("par3", true);
        assertOutputExists("last", true);
    }
    */

    @Test
    public void testStartLast()
            throws Exception
    {
        run("--start", "+last");
        assertOutputExists("seq1", false);
        assertOutputExists("seq2", false);
        assertOutputExists("seq3", false);
        assertOutputExists("par1", false);
        assertOutputExists("par2", false);
        assertOutputExists("par3", false);
        assertOutputExists("last", true);
    }

    @Test
    public void testGoalSeq1()
            throws Exception
    {
        run("--goal", "+seq1");
        assertOutputExists("seq1", true);
        assertOutputExists("seq2", false);
        assertOutputExists("seq3", false);
        assertOutputExists("par1", false);
        assertOutputExists("par2", false);
        assertOutputExists("par3", false);
        assertOutputExists("last", false);
    }

    @Test
    public void testGoalSeq2()
            throws Exception
    {
        run("--goal", "+seq2");
        assertOutputExists("seq1", false);
        assertOutputExists("seq2", true);
        assertOutputExists("seq3", false);
        assertOutputExists("par1", false);
        assertOutputExists("par2", false);
        assertOutputExists("par3", false);
        assertOutputExists("last", false);
    }

    @Test
    public void testGoalSeq2Nested()
            throws Exception
    {
        run("--goal", "+seq2+nest");
        assertOutputExists("seq1", false);
        assertOutputExists("seq2", true);
        assertOutputExists("seq3", false);
        assertOutputExists("par1", false);
        assertOutputExists("par2", false);
        assertOutputExists("par3", false);
        assertOutputExists("last", false);
    }

    @Test
    public void testGoalLast()
            throws Exception
    {
        run("--goal", "+last");
        assertOutputExists("seq1", false);
        assertOutputExists("seq2", false);
        assertOutputExists("seq3", false);
        assertOutputExists("par1", false);
        assertOutputExists("par2", false);
        assertOutputExists("par3", false);
        assertOutputExists("last", true);
    }
}
