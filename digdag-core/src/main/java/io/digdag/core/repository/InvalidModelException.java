package io.digdag.core.repository;

public class InvalidModelException
    extends RuntimeException
{
    private final Object modelObject;

    public InvalidModelException(Object modelObject, String message)
    {
        super(message);
        this.modelObject = modelObject;
    }

    public Object getModelObject()
    {
        return modelObject;
    }
}
