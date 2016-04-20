package io.digdag.core.agent;

import java.util.*;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import org.skife.jdbi.v2.IDBI;
import org.junit.*;
import com.google.common.base.Optional;
import com.google.common.collect.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static io.digdag.core.database.DatabaseTestingUtils.*;
import static org.junit.Assert.*;

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
