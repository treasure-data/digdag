package io.digdag.client.api;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class IdTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testAsIntAndLong()
            throws Exception
    {
        assertThat(Id.of("1").asInt(), is(1));
        assertThat(Id.of("2").asInt(), is(2));
        assertThat(Id.of("1").asLong(), is(1L));
    }

    @Test
    public void testIntOverflow()
            throws Exception
    {
        exception.expect(NumberFormatException.class);
        Id.of("2147483648").asInt();
    }

    @Test
    public void testLongOverflow()
            throws Exception
    {
        exception.expect(NumberFormatException.class);
        Id.of("-9223372036854775809").asLong();
    }
}
