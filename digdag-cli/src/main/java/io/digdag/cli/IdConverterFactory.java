package io.digdag.cli;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.IStringConverterFactory;
import com.beust.jcommander.converters.LongConverter;
import io.digdag.client.api.Id;

public class IdConverterFactory
    implements IStringConverterFactory
{
    @Override
    @SuppressWarnings("unchecked")
    public <T> Class<? extends IStringConverter<T>> getConverter(Class<T> forType)
    {
        if (forType.equals(Id.class)) {
            return (Class<IStringConverter<T>>) (Class<?>) IdConverter.class;
        }
        return null;
    }

    private static class IdConverter
        implements IStringConverter<Id>
    {
        private final LongConverter longConverter;

        public IdConverter(String fieldName)
        {
            this.longConverter = new LongConverter(fieldName);
        }

        @Override
        public Id convert(String value)
        {
            return Id.of(Long.toString(longConverter.convert(value)));
        }
    }
}
