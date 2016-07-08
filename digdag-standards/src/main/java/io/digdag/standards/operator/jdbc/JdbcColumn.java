package io.digdag.standards.operator.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JdbcColumn
{
    private String name;
    private String typeName;
    private int sqlType;
    private TypeGroup typeGroup;
    private int precision;
    private int scale;

    @JsonCreator
    public JdbcColumn(
            @JsonProperty("name") String name,
            @JsonProperty("typeName") String typeName,
            @JsonProperty("sqlType") int sqlType,
            @JsonProperty("typeGroup") TypeGroup typeGroup,
            @JsonProperty("precision") int precision,
            @JsonProperty("scale") int scale)
    {
        this.name = name;
        this.typeName = typeName;
        this.sqlType = sqlType;
        this.typeGroup = typeGroup;
        this.precision = precision;
        this.scale = scale;
    }

    @JsonProperty("name")
    public String getName()
    {
        return name;
    }

    @JsonProperty("typeName")
    public String getTypeName()
    {
        return typeName;
    }

    @JsonProperty("sqlType")
    public int getSqlType()
    {
        return sqlType;
    }

    @JsonProperty("typeGroup")
    public TypeGroup getTypeGroup()
    {
        return typeGroup;
    }

    @JsonProperty("precision")
    public int getPrecision()
    {
        return precision;
    }

    @JsonProperty("scale")
    public int getScale()
    {
        return scale;
    }

    @Override
    public String toString()
    {
        return "JdbcColumn{" +
                "name='" + name + '\'' +
                ", typeName='" + typeName + '\'' +
                ", sqlType=" + sqlType +
                ", typeGroup=" + typeGroup +
                ", precision=" + precision +
                ", scale=" + scale +
                '}';
    }
}
