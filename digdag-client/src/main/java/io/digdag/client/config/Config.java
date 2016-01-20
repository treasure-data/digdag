package io.digdag.client.config;

import java.util.List;
import java.util.Map;
import java.util.Iterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.core.type.TypeReference;

public class Config
{
    protected final ObjectMapper mapper;
    protected final ObjectNode object;

    public Config(ObjectMapper mapper)
    {
        this(mapper, new ObjectNode(JsonNodeFactory.instance));
    }

    public Config(ObjectMapper mapper, JsonNode object)
    {
        this.mapper = mapper;
        this.object = (ObjectNode) object;
    }

    // here uses JsonNode instead of ObjectNode for workaround of https://github.com/FasterXML/jackson-databind/issues/941
    @JsonCreator
    public static Config deserializeFromJackson(@JacksonInject ObjectMapper mapper, JsonNode object)
    {
        if (!object.isObject()) {
            throw new RuntimeJsonMappingException("Expected object but got "+object);
        }
        return new Config(mapper, (ObjectNode) object);
    }

    @JsonValue
    public ObjectNode getInternalObjectNode()
    {
        return object;
    }

    public Config set(String key, Object v)
    {
        if (v == null) {
            remove(key);
        } else {
            object.set(key, writeObject(v));
        }
        return this;
    }

    public Config setNested(String key, Config v)
    {
        object.set(key, v.object);
        return this;
    }

    public Config setAll(Config other)
    {
        for (Map.Entry<String, JsonNode> field : other.getEntries()) {
            object.set(field.getKey(), field.getValue());
        }
        return this;
    }

    private Iterable<Map.Entry<String, JsonNode>> getEntries()
    {
        return new Iterable<Map.Entry<String, JsonNode>>() {
            public Iterator<Map.Entry<String, JsonNode>> iterator()
            {
                return object.fields();
            }
        };
    }

    public Config remove(String key)
    {
        object.remove(key);
        return this;
    }

    public Config deepCopy()
    {
        return new Config(mapper, object.deepCopy());
    }

    //public Config merge(Config other)
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

    public ConfigFactory getFactory()
    {
        return new ConfigFactory(mapper);
    }

    public List<String> getKeys()
    {
        return ImmutableList.copyOf(object.fieldNames());
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
    public <E> Optional<E> getOptional(String key, Class<E> type)
    {
        return (Optional<E>) get(key, mapper.getTypeFactory().constructParametrizedType(Optional.class, Optional.class, type), Optional.<E>absent());
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

    public Config getNested(String key)
    {
        JsonNode value = object.get(key);
        if (value == null) {
            throw new ConfigException("Attribute "+key+" is required but not set");
        }
        if (!value.isObject()) {
            throw new ConfigException("Attribute "+key+" must be an object");
        }
        return new Config(mapper, (ObjectNode) value);
    }

    public Config getNestedOrSetEmpty(String key)
    {
        JsonNode value = object.get(key);
        if (value == null) {
            value = object.objectNode();
            object.set(key, value);
        }
        else if (!value.isObject()) {
            throw new ConfigException("Attribute "+key+" must be an object");
        }
        return new Config(mapper, (ObjectNode) value);
    }

    public Config getNestedOrGetEmpty(String key)
    {
        JsonNode value = object.get(key);
        if (value == null) {
            value = object.objectNode();
        }
        else if (!value.isObject()) {
            throw new ConfigException("Attribute "+key+" must be an object");
        }
        return new Config(mapper, (ObjectNode) value);
    }

    public Config getNestedOrderedOrGetEmpty(String key)
    {
        JsonNode value = object.get(key);
        if (value == null) {
            value = object.objectNode();
        }
        else if (value.isArray()) {
            Config config = new Config(mapper);
            Iterator<JsonNode> ite = ((ArrayNode) value).elements();
            while (ite.hasNext()) {
                JsonNode nested = ite.next();
                if (!(nested instanceof ObjectNode)) {
                    throw new RuntimeJsonMappingException("Expected object but got "+nested);
                }
                // here assumes config is an order-preserving map
                config.setAll(new Config(mapper, (ObjectNode) nested));
            }
            return config;
        }
        else if (!value.isObject()) {
            throw new ConfigException("Attribute "+key+" must be an object or array of objects");
        }
        return new Config(mapper, (ObjectNode) value);
    }

    private <E> E readObject(Class<E> type, JsonNode value)
    {
        try {
            return mapper.readValue(value.traverse(), type);
        }
        catch (Exception ex) {
            Throwables.propagateIfInstanceOf(ex, ConfigException.class);
            throw new ConfigException(ex);
        }
    }

    private Object readObject(JavaType type, JsonNode value)
    {
        try {
            return mapper.readValue(value.traverse(), type);
        }
        catch (Exception ex) {
            Throwables.propagateIfInstanceOf(ex, ConfigException.class);
            throw new ConfigException(ex);
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
        if (!(other instanceof Config)) {
            return false;
        }
        return object.equals(((Config) other).object);
    }

    @Override
    public int hashCode()
    {
        return object.hashCode();
    }
}
