package io.digdag.commons;

import static org.junit.Assert.fail;

public class AssertUtil {
    public static <E> void assertException(Runnable func1, Class<E> expected, String failMessage)
    {
        try {
            func1.run();
            fail(failMessage);
        }
        catch (Exception e){
            if (e.getClass().equals(expected)) {
                //OK
            }
            else {
                fail("Unexpected exception:" + e);
            }
        }
    }
}
