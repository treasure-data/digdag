package io.digdag.metrics;

/**
 *  This code come from micrometer Timed.java to add member to original Timed
 */

import java.lang.annotation.*;

@Target({ElementType.ANNOTATION_TYPE, ElementType.TYPE, ElementType.METHOD})
@Repeatable(DigdagTimedSet.class)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface DigdagTimed {
    String category() default "default";

    String value() default "";

    String[] extraTags() default {};

    boolean longTask() default false;

    double[] percentiles() default {};

    boolean histogram() default false;

    String description() default "";

    boolean taskRequest() default false;

    boolean appendMethodName() default false;
}

