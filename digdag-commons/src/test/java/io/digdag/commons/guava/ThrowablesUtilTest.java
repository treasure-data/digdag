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

    @Test
    public void propagateIfInstanceOfBehavior()
    {
        // Do not anything if null
        ThrowablesUtil.propagateIfInstanceOf(null, RuntimeException.class);

        Assert.assertThrows(NullPointerException.class, () -> ThrowablesUtil.propagateIfInstanceOf(new NullPointerException("test"), NullPointerException.class));
        //NullPointerException is inheritance of RuntimeException
        Assert.assertThrows(NullPointerException.class, () -> ThrowablesUtil.propagateIfInstanceOf(new NullPointerException("test"), RuntimeException.class));
        // NullPointerException and Error has no relation. So not throw
        ThrowablesUtil.propagateIfInstanceOf(new NullPointerException("test"), Error.class);
    }

    @Test
    public void propagateIfInstanceOfCompatibility()
    {
        comparePropagateIfInstanceOf(null, NullPointerException.class);
        comparePropagateIfInstanceOf(new NullPointerException("test"), NullPointerException.class);
        comparePropagateIfInstanceOf(new RuntimeException("test"), RuntimeException.class);
        comparePropagateIfInstanceOf(new AssertionError("test"), AssertionError.class);
        comparePropagateIfInstanceOf(new IOException("test"), IOException.class);
    }

    @Test
    public void propagateIfPossibleBehavior()
    {
        // Do not anything if null
        ThrowablesUtil.propagateIfPossible(null);
        //Unchecked exception will be through as is.
        Assert.assertThrows(NullPointerException.class, () -> ThrowablesUtil.propagateIfPossible(new NullPointerException("test")));
        Assert.assertThrows(RuntimeException.class, () -> ThrowablesUtil.propagateIfPossible(new RuntimeException("test")));
        Assert.assertThrows(AssertionError.class, () -> ThrowablesUtil.propagateIfPossible(new AssertionError("test")));
        //Checked exception will not be through
        ThrowablesUtil.propagateIfPossible(new IOException("test"));
    }

    @Test
    public void propagateIfPossibleCompatibility()
    {
        comparePropagateIfPossible(null);
        comparePropagateIfPossible(new NullPointerException("test"));
        comparePropagateIfPossible(new RuntimeException("test"));
        comparePropagateIfPossible(new AssertionError("test"));
        comparePropagateIfPossible(new IOException("test"));
    }

    @SuppressWarnings("deprecation")
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

    @SuppressWarnings("deprecation")
    private <X extends Throwable> void comparePropagateIfInstanceOf(Throwable s, Class<X> c)
    {
        Throwable t1 = null;
        Throwable t2 = null;
        try {
            Throwables.propagateIfInstanceOf(s, c);
        }
        catch (Throwable e) {
            t1 = e;
        }
        try {
            ThrowablesUtil.propagateIfInstanceOf(s, c);
        }
        catch (Throwable e) {
            t2 = e;
        }
        if (t1 == null && t2 == null) {
            //OK
        }
        else {
            Assert.assertEquals(t1.getClass().getCanonicalName(), t1.getClass().getCanonicalName());
            Assert.assertEquals(t1.getMessage(), t2.getMessage());
        }
    }

    @SuppressWarnings("deprecation")
    private void comparePropagateIfPossible(Throwable s)
    {
        Throwable t1 = null;
        Throwable t2 = null;
        try {
            Throwables.propagateIfPossible(s);
        }
        catch (Throwable e) {
            t1 = e;
        }
        try {
            ThrowablesUtil.propagateIfPossible(s);
        }
        catch (Throwable e) {
            t2 = e;
        }
        if (t1 == null && t2 == null) {
            //OK
        }
        else {
            Assert.assertEquals(t1.getClass().getCanonicalName(), t1.getClass().getCanonicalName());
            Assert.assertEquals(t1.getMessage(), t2.getMessage());
        }
    }
}
