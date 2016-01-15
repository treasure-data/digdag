package io.digdag.core.repository;

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
}
