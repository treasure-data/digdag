package io.digdag.commons.guava;

import com.google.common.base.Throwables;

public class ThrowablesUtil
{

    /**
     *   Original code from com.google.common.base.Throwables.propagate
     *   This method is provided for reduce guava dependencies, suppress many warnings on deprecation.
     *   As the wiki describe, propagate() is not necessary in most of cases.
     *   So not use this method for new updated code.
     *   Reference: https://github.com/google/guava/wiki/Why-we-deprecated-Throwables.propagate
     */
    public static RuntimeException propagate(Throwable throwable) {
        Throwables.throwIfUnchecked(throwable);
        throw new RuntimeException(throwable);
    }
}
