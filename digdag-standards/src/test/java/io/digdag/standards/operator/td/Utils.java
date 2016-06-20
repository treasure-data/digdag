package io.digdag.standards.operator.td;

import org.hamcrest.Matchers;

import java.util.concurrent.Callable;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

class Utils
{
    static <T, E extends Throwable> void assertThrows(Callable<T> callable, Class<E> exceptionClass)
    {
        try {
            callable.call();
        }
        catch (Throwable e) {
            assertThat(e, Matchers.instanceOf(exceptionClass));
            return;
        }
        fail("Expected an exception of type: " + exceptionClass);
    }
}
