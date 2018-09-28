package io.digdag.spi;

public enum ValueType
{
    STRING,
    ARRAY;

    public static ValueType of(int code)
    {
        switch (code) {
            case 0:
                return ValueType.STRING;
            case 1:
                return ValueType.ARRAY;
            default:
                throw new IllegalArgumentException("Unexpected valueType code: " + code);
        }
    }
}
