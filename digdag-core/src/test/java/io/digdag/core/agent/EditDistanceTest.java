package io.digdag.core.agent;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class EditDistanceTest
{
    @Test
    public void testDistances()
        throws Exception
    {
        assertDistance(0, "a", "a");
        assertDistance(1, "", "a");
        assertDistance(1, "ab", "abc");
        assertDistance(1, "ab", "acb");
        assertDistance(1, "ab", "ac");
        assertDistance(2, "ab", "acd");
        assertDistance(3, "a", "cbda");
    }

    private void assertDistance(int expected, String str1, String str2)
    {
        assertEquals(expected, EditDistance.distance(str1, str2));
        assertEquals(expected, EditDistance.distance(str2, str1));
    }
}
