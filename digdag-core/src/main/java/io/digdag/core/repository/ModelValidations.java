package io.digdag.core.repository;

import java.util.Locale;

public final class ModelValidations
{
    private ModelValidations()
    { }

    public static void check(Object modelObject, boolean expression, String errorMessage)
    {
        if (!expression) {
            throw new InvalidModelException(modelObject, errorMessage);
        }
    }

    public static void check(Object modelObject, boolean expression, String errorMessageFormat, Object... objects)
    {
        if (!expression) {
            throw new InvalidModelException(modelObject, String.format(errorMessageFormat, Locale.ENGLISH, objects));
        }
    }
}
