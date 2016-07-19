package io.digdag.core.agent;

import com.google.common.collect.ImmutableList;
import io.digdag.spi.SecretSelector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SecretFilterTest
{
    @Mock SecretSelector matchingSelector;
    @Mock SecretSelector nonMatchingSelector;

    @Before
    public void setUp()
            throws Exception
    {
        when(matchingSelector.match(anyString())).thenReturn(true);
        when(nonMatchingSelector.match(anyString())).thenReturn(false);
    }

    @Test
    public void match()
            throws Exception
    {
        assertThat(SecretFilter.of(emptyList()).match("foobar"), is(false));
        assertThat(SecretFilter.of(ImmutableList.of(nonMatchingSelector)).match("bar"), is(false));
        assertThat(SecretFilter.of(ImmutableList.of(nonMatchingSelector, nonMatchingSelector)).match("bar"), is(false));

        assertThat(SecretFilter.of(ImmutableList.of(matchingSelector)).match("bar"), is(true));
        assertThat(SecretFilter.of(ImmutableList.of(nonMatchingSelector, matchingSelector)).match("bar"), is(true));
        assertThat(SecretFilter.of(ImmutableList.of(matchingSelector, nonMatchingSelector)).match("bar"), is(true));
    }
}
