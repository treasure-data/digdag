package io.digdag.commons.guava;

import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ThrowablesUtilTest
{
    @Test
    public void propagateBehavior()
    {
        // null will be converted to NullPointerException
        Assert.assertThrows(NullPointerException.class, () -> ThrowablesUtil.propagate(null));
        //Unchecked exception will be through as is.
        Assert.assertThrows(NullPointerException.class, () -> ThrowablesUtil.propagate(new NullPointerException("test")));
        Assert.assertThrows(RuntimeException.class, () -> ThrowablesUtil.propagate(new RuntimeException("test")));
        Assert.assertThrows(AssertionError.class, () -> ThrowablesUtil.propagate(new AssertionError("test")));
        //Checked exception will be converted to RuntimeException
        Assert.assertThrows(RuntimeException.class, () -> ThrowablesUtil.propagate(new IOException("test")));
    }

    @Test
    public void propagateCompatibility()
    {
        comparePropagate(null);
        comparePropagate(new NullPointerException("test"));
        comparePropagate(new RuntimeException("test"));
        comparePropagate(new AssertionError("test"));
        comparePropagate(new IOException("test"));
    }

    @SuppressWarnings( "deprecation" )
    private void comparePropagate(Throwable s)
    {
        Throwable t1 = null;
        Throwable t2 = null;
        try {
            Throwables.propagate(s);
        }
        catch (Throwable e) {
            t1 = e;
        }
        try {
            ThrowablesUtil.propagate(s);
        }
        catch (Throwable e) {
            t2 = e;
        }
        Assert.assertEquals(t1.getClass().getCanonicalName(), t1.getClass().getCanonicalName());
        Assert.assertEquals(t1.getMessage(), t2.getMessage());
    }
}
