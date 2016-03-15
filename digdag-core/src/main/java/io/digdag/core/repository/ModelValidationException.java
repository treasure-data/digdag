package io.digdag.core.repository;

import java.util.List;

public class ModelValidationException
    extends IllegalStateException
{
    public static class Failure
    {
        private final String fieldName;
        private final Object object;
        private final String message;

        public Failure(String fieldName, Object object, String message)
        {
            this.fieldName = fieldName;
            this.object = object;
            this.message = message;
        }

        public String getFieldName()
        {
            return fieldName;
        }

        public Object getObject()
        {
            return object;
        }

        public String getMessage()
        {
            return message;
        }
    }

    private final Object modelObject;
    private final List<Failure> failures;

    public ModelValidationException(String message, Object modelObject, List<Failure> failures)
    {
        super(buildMessage(message, failures));
        this.modelObject = modelObject;
        this.failures = failures;
    }

    private static String buildMessage(String message, List<Failure> failures)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(message);
        sb.append("\n");
        for (Failure failure : failures) {
            sb.append(failure.getFieldName());
            sb.append(' ');
            sb.append(failure.getMessage());
            sb.append(" \"");
            sb.append(failure.getObject() == null ? "null" : failure.getObject().toString());
            sb.append('\"');
            sb.append('\n');
        }
        return sb.toString();
    }

    public Object getModelObject()
    {
        return modelObject;
    }

    public List<Failure> getFailures()
    {
        return failures;
    }
}
