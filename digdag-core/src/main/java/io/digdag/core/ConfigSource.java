package io.digdag.core;

import java.util.List;
import java.util.Map;
import java.util.Iterator;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.core.type.TypeReference;

public class ConfigSource
{
    protected final ObjectMapper mapper;
    protected final ObjectNode object;

    public ConfigSource(ObjectMapper mapper)
    {
        this(mapper, new ObjectNode(JsonNodeFactory.instance));
    }

    public ConfigSource(ObjectMapper mapper, JsonNode object)
    {
        this.mapper = mapper;
        this.object = (ObjectNode) object;
    }

    // uses JsonNode instead of ObjectNode for workaround of https://github.com/FasterXML/jackson-databind/issues/941
    @JsonCreator
    public static ConfigSource _deserializeFromJackson(@JacksonInject ObjectMapper mapper, JsonNode object)
    {
        if (!(object instanceof ObjectNode)) {
            throw new RuntimeJsonMappingException("Expected object but got "+object);
        }
        return new ConfigSource(mapper, (ObjectNode) object);
    }

    @JsonValue
    public ObjectNode getInternalObjectNode()
    {
        return object;
    }

    public ConfigSourceFactory getFactory()
    {
        return new ConfigSourceFactory(mapper);
    }

    public ConfigSource newConfigSource()
    {
        return new ConfigSource(mapper);
    }

    public List<String> getKeys()
    {
        return ImmutableList.copyOf(object.fieldNames());
    }

    public Iterable<Map.Entry<String, JsonNode>> getEntries()
    {
        return new Iterable<Map.Entry<String, JsonNode>>() {
            public Iterator<Map.Entry<String, JsonNode>> iterator()
            {
                return object.fields();
            }
        };
    }

    public boolean isEmpty()
    {
        return !object.fieldNames().hasNext();
    }

    public boolean has(String key)
    {
        return object.has(key);
    }

    public <E> E convert(Class<E> type)
    {
        return readObject(type, object);
    }

    public <E> E get(String key, Class<E> type)
    {
        JsonNode value = object.get(key);
        if (value == null) {
            throw new ConfigException("Attribute "+key+" is required but not set");
        }
        return readObject(type, value);
    }

    public Object get(String key, JavaType type)
    {
        JsonNode value = object.get(key);
        if (value == null) {
            throw new ConfigException("Attribute "+key+" is required but not set");
        }
        return readObject(type, value);
    }

    @SuppressWarnings("unchecked")
    public <E> E get(String key, TypeReference<E> type)
    {
        return (E) get(key, mapper.getTypeFactory().constructType(type));
    }

    public <E> E get(String key, Class<E> type, E defaultValue)
    {
        JsonNode value = object.get(key);
        if (value == null) {
            return defaultValue;
        }
        return readObject(type, value);
    }

    public Object get(String key, JavaType type, Object defaultValue)
    {
        JsonNode value = object.get(key);
        if (value == null) {
            return defaultValue;
        }
        return readObject(type, value);
    }

    @SuppressWarnings("unchecked")
    public <E> E get(String key, TypeReference<E> type, E defaultValue)
    {
        return (E) get(key, mapper.getTypeFactory().constructType(type));
    }

    @SuppressWarnings("unchecked")
    public <E> List<E> getList(String key, Class<E> elementType)
    {
        return (List<E>) get(key, mapper.getTypeFactory().constructParametrizedType(List.class, List.class, elementType));
    }

    @SuppressWarnings("unchecked")
    public <E> List<E> getListOrEmpty(String key, Class<E> elementType)
    {
        return (List<E>) get(key, mapper.getTypeFactory().constructParametrizedType(List.class, List.class, elementType), ImmutableList.<E>of());
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(String key, Class<K> keyType, Class<V> valueType)
    {
        return (Map<K, V>) get(key, mapper.getTypeFactory().constructParametrizedType(Map.class, Map.class, keyType, valueType));
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMapOrEmpty(String key, Class<K> keyType, Class<V> valueType)
    {
        return (Map<K, V>) get(key, mapper.getTypeFactory().constructParametrizedType(Map.class, Map.class, keyType, valueType), ImmutableMap.<K, V>of());
    }

    public ConfigSource getNested(String key)
    {
        JsonNode value = object.get(key);
        if (value == null) {
            throw new ConfigException("Attribute "+key+" is required but not set");
        }
        if (!value.isObject()) {
            throw new ConfigException("Attribute "+key+" must be an object");
        }
        return new ConfigSource(mapper, (ObjectNode) value);
    }

    public ConfigSource getNestedOrSetEmpty(String key)
    {
        JsonNode value = object.get(key);
        if (value == null) {
            value = object.objectNode();
            object.set(key, value);
        } else if (!value.isObject()) {
            throw new ConfigException("Attribute "+key+" must be an object");
        }
        return new ConfigSource(mapper, (ObjectNode) value);
    }

    public ConfigSource getNestedOrGetEmpty(String key)
    {
        JsonNode value = object.get(key);
        if (value == null) {
            value = object.objectNode();
        } else if (!value.isObject()) {
            throw new ConfigException("Attribute "+key+" must be an object");
        }
        return new ConfigSource(mapper, (ObjectNode) value);
    }

    public ConfigSource set(String key, Object v)
    {
        if (v == null) {
            remove(key);
        } else {
            object.set(key, writeObject(v));
        }
        return this;
    }

    public ConfigSource setNested(String key, ConfigSource v)
    {
        object.set(key, v.object);
        return this;
    }

    public ConfigSource setAll(ConfigSource other)
    {
        for (Map.Entry<String, JsonNode> field : other.getEntries()) {
            object.set(field.getKey(), field.getValue());
        }
        return this;
    }

    public ConfigSource remove(String key)
    {
        object.remove(key);
        return this;
    }

    public ConfigSource deepCopy()
    {
        return new ConfigSource(mapper, object.deepCopy());
    }

    //public ConfigSource merge(ConfigSource other)
    //{
    //    mergeJsonObject(object, other.deepCopy().object);
    //    return this;
    //}

    //private static void mergeJsonObject(ObjectNode src, ObjectNode other)
    //{
    //    Iterator<Map.Entry<String, JsonNode>> ite = other.fields();
    //    while (ite.hasNext()) {
    //        Map.Entry<String, JsonNode> pair = ite.next();
    //        JsonNode s = src.get(pair.getKey());
    //        JsonNode v = pair.getValue();
    //        if (v.isObject() && s != null && s.isObject()) {
    //            mergeJsonObject((ObjectNode) s, (ObjectNode) v);
    //        } else if (v.isArray() && s != null && s.isArray()) {
    //            mergeJsonArray((ArrayNode) s, (ArrayNode) v);
    //        } else {
    //            src.replace(pair.getKey(), v);
    //        }
    //    }
    //}

    //private static void mergeJsonArray(ArrayNode src, ArrayNode other)
    //{
    //    for (int i=0; i < other.size(); i++) {
    //        JsonNode s = src.get(i);
    //        JsonNode v = other.get(i);
    //        if (s == null) {
    //            src.add(v);
    //        } else if (v.isObject() && s.isObject()) {
    //            mergeJsonObject((ObjectNode) s, (ObjectNode) v);
    //        } else if (v.isArray() && s.isArray()) {
    //            mergeJsonArray((ArrayNode) s, (ArrayNode) v);
    //        } else {
    //            src.remove(i);
    //            src.insert(i, v);
    //        }
    //    }
    //}

    private <E> E readObject(Class<E> type, JsonNode value)
    {
        try {
            return mapper.readValue(value.traverse(), type);
        }
        catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    private Object readObject(JavaType type, JsonNode value)
    {
        try {
            return mapper.readValue(value.traverse(), type);
        }
        catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    private JsonNode writeObject(Object obj)
    {
        try {
            String value = mapper.writeValueAsString(obj);
            return mapper.readTree(value);
        }
        catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public String toString()
    {
        return object.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof ConfigSource)) {
            return false;
        }
        return object.equals(((ConfigSource) other).object);
    }

    @Override
    public int hashCode()
    {
        return object.hashCode();
    }
}
