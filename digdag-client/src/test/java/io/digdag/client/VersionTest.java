package io.digdag.client;

import org.junit.Test;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.hamcrest.MatcherAssert.assertThat;

public class VersionTest
{
    @Test
    public void testValidVersions()
    {
        assertValidVersion("0");
        assertValidVersion("1");
        assertValidVersion("10");
        assertValidVersion("0.2");
        assertValidVersion("0.23");
        assertValidVersion("0.4.5");
        assertValidVersion("0.4.56");
        assertValidVersion("0.4.50");
        assertValidVersion("0.0.6");
        assertValidVersion("0.0.6.7");
        assertValidVersion("8.9.10.11");
        assertValidVersion("0-SNAPSHOT");
        assertValidVersion("0.8.32-20170117T115910-a60cad1343c71489d752f39250470e10109e3425");
        assertValidVersion(DigdagVersion.buildVersion().toString());
    }

    private void assertValidVersion(String versionString)
    {
        Version parsed;
        try {
            parsed = Version.parse(versionString);
        }
        catch (IllegalArgumentException ex) {
            fail();
            throw ex;
        }
        assertThat(parsed.toString(), is(versionString));
        assertTrue(parsed.equals(Version.parse(versionString)));
        assertFalse(parsed.isOlder(Version.parse(versionString)));
        assertFalse(parsed.isNewer(Version.parse(versionString)));
        assertThat(parsed.compareTo(Version.parse(versionString)), is(0));
    }

    @Test
    public void testInvalidVersions()
    {
        assertInvalidVersion("", "Invalid version string: ");
        assertInvalidVersion("10.02", "Invalid version string: 10.02");
        assertInvalidVersion("00", "Invalid version string: 00");
        assertInvalidVersion("01", "Invalid version string: 01");
        assertInvalidVersion("-SNAPSHOT", "Invalid version string: -SNAPSHOT");
        assertInvalidVersion("0.1v", "Invalid version string: 0.1v");
        assertInvalidVersion("0..1", "Invalid version string: 0..1");
        assertInvalidVersion("1..", "Invalid version string: 1..");
        assertInvalidVersion("932132232132132131", "Too big version number: 932132232132132131");
    }

    private void assertInvalidVersion(String versionString, String message)
    {
        try {
            Version.parse(versionString);
            fail();
        }
        catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is(message));
        }
    }

    @Test
    public void testOrder()
    {
        assertNewer("0.1", "0.2");
        assertNewer("0.1", "0.1.1");
        assertNewer("0.2", "0.10");
        assertNewer("0", "0.10");
        assertNewer("1.0-rc1", "1.0");
        assertNewer("1.0-2016T01", "1.0-2016T02");
        assertNewer("1.0-2016T01", "1.0-2016T01-02");
    }

    private void assertNewer(String older, String newer)
    {
        Version v1 = Version.parse(older);
        Version v2 = Version.parse(newer);

        assertTrue(v1.isOlder(v2));
        assertFalse(v1.isNewer(v2));
        assertThat(v1.compareTo(v2), is(-1));
        assertFalse(v1.equals(v2));

        assertFalse(v2.isOlder(v1));
        assertTrue(v2.isNewer(v1));
        assertThat(v2.compareTo(v1), is(1));
        assertFalse(v2.equals(v1));
    }

    @Test
    public void testSameOrder()
    {
        assertSameOrder("0", "0.0");
        assertSameOrder("1", "1.0");
        assertSameOrder("1.0", "1.0.0");
    }

    private void assertSameOrder(String a, String b)
    {
        Version v1 = Version.parse(a);
        Version v2 = Version.parse(b);

        assertFalse(v1.isOlder(v2));
        assertFalse(v1.isNewer(v2));
        assertThat(v1.compareTo(v2), is(0));

        assertFalse(v2.isOlder(v1));
        assertFalse(v2.isNewer(v1));
        assertThat(v2.compareTo(v1), is(0));
    }
}
