package io.digdag.client.config;

import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.io.IOException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.base.Optional;
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
import io.digdag.commons.guava.ThrowablesUtil;

import static java.util.Locale.ENGLISH;

public class Config
{
    protected final ObjectMapper mapper;
    protected final ObjectNode object;

    Config(ObjectMapper mapper)
    {
        this(mapper, new ObjectNode(JsonNodeFactory.instance));
    }

    Config(ObjectMapper mapper, JsonNode object)
    {
        this.mapper = mapper;
        this.object = (ObjectNode) object;
    }

    protected Config(Config config)
    {
        this.mapper = config.mapper;
        this.object = config.object.deepCopy();
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
            setNode(key, writeObject(v));
        }
        return this;
    }

    public Config setOptional(String key, Optional<?> v)
    {
        if (v.isPresent()) {
            set(key, v.get());
        }
        return this;
    }

    public Config setIfNotSet(String key, Object v)
    {
        if (!has(key)) {
            if (v != null) {
                setNode(key, writeObject(v));
            }
        }
        return this;
    }

    public Config setNested(String key, Config v)
    {
        setNode(key, v.object);
        return this;
    }

    public Config setAll(Config other)
    {
        for (Map.Entry<String, JsonNode> field : other.getEntries()) {
            setNode(field.getKey(), field.getValue());
        }
        return this;
    }

    public Config setAllIfNotSet(Config other)
    {
        for (Map.Entry<String, JsonNode> field : other.getEntries()) {
            setIfNotSet(field.getKey(), field.getValue());
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
        return new Config(this);
    }

    public Config merge(Config other)
    {
        mergeJsonObject(object, other.deepCopy().object);
        return this;
    }

    public Config mergeDefault(Config other)
    {
        mergeDefaultJsonObject(object, other.deepCopy().object);
        return this;
    }

    private static void mergeJsonObject(ObjectNode src, ObjectNode other)
    {
        Iterator<Map.Entry<String, JsonNode>> ite = other.fields();
        while (ite.hasNext()) {
            Map.Entry<String, JsonNode> pair = ite.next();
            JsonNode s = src.get(pair.getKey());
            JsonNode v = pair.getValue();
            if (v.isObject() && s != null && s.isObject()) {
                mergeJsonObject((ObjectNode) s, (ObjectNode) v);
            } else {
                src.set(pair.getKey(), v);  // keeps order if key exists
            }
        }
    }

    private static void mergeDefaultJsonObject(ObjectNode src, ObjectNode other)
    {
        Iterator<Map.Entry<String, JsonNode>> ite = other.fields();
        while (ite.hasNext()) {
            Map.Entry<String, JsonNode> pair = ite.next();
            JsonNode s = src.get(pair.getKey());
            JsonNode v = pair.getValue();
            if (v.isObject() && s != null && s.isObject()) {
                mergeDefaultJsonObject((ObjectNode) s, (ObjectNode) v);
            } else if (s == null) {
                src.set(pair.getKey(), v);
            }
        }
    }

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
            throw ThrowablesUtil.propagate(ex);
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
        return readObject(type, object, null);
    }

    public <E> E get(String key, Class<E> type)
    {
        JsonNode value = getNode(key);
        if (value == null) {
            throw new ConfigException("Parameter '"+key+"' is required but not set");
        }
        else if (value.isNull()) {
            throw new ConfigException("Parameter '"+key+"' is required but null");
        }
        return readObject(type, value, key);
    }

    public Object get(String key, JavaType type)
    {
        JsonNode value = getNode(key);
        if (value == null) {
            throw new ConfigException("Parameter '"+key+"' is required but not set");
        }
        else if (value.isNull()) {
            throw new ConfigException("Parameter '"+key+"' is required but null");
        }
        return readObject(type, value, key);
    }

    @SuppressWarnings("unchecked")
    public <E> E get(String key, TypeReference<E> type)
    {
        return (E) get(key, mapper.getTypeFactory().constructType(type));
    }

    public <E> E get(String key, Class<E> type, E defaultValue)
    {
        JsonNode value = getNode(key);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        return readObject(type, value, key);
    }

    public Object get(String key, JavaType type, Object defaultValue)
    {
        JsonNode value = getNode(key);
        if (value == null || value.isNull()) {
            return defaultValue;
        }
        return readObject(type, value, key);
    }

    @SuppressWarnings("unchecked")
    public <E> E get(String key, TypeReference<E> type, E defaultValue)
    {
        return (E) get(key, mapper.getTypeFactory().constructType(type), defaultValue);
    }

    @SuppressWarnings("unchecked")
    public <E> Optional<E> getOptional(String key, Class<E> type)
    {
        return (Optional<E>) get(key,
                mapper.getTypeFactory().constructReferenceType(Optional.class, mapper.getTypeFactory().constructType(type)),
                Optional.<E>absent());
    }

    @SuppressWarnings("unchecked")
    public <E> Optional<E> getOptional(String key, TypeReference<E> type)
    {
        return (Optional<E>) get(key, mapper.getTypeFactory().constructType(type), Optional.<E>absent());
    }

    @SuppressWarnings("unchecked")
    public <E> List<E> getList(String key, Class<E> elementType)
    {
        return (List<E>) get(key, mapper.getTypeFactory().constructCollectionType(List.class, elementType));
    }

    @SuppressWarnings("unchecked")
    public <E> List<E> getListOrEmpty(String key, Class<E> elementType)
    {
        return (List<E>) get(key, mapper.getTypeFactory().constructCollectionType(List.class, elementType), ImmutableList.<E>of());
    }

    @SuppressWarnings("unchecked")
    public <E> List<E> parseList(String key, Class<E> elementType)
    {
        JsonNode parsed = tryParseNested(key);
        if (parsed == null) {
            return getList(key, elementType);
        }
        else {
            return (List<E>) readObject(mapper.getTypeFactory().constructCollectionType(List.class, elementType), parsed, key);
        }
    }

    @SuppressWarnings("unchecked")
    public <E> List<E> parseListOrGetEmpty(String key, Class<E> elementType)
    {
        JsonNode parsed = tryParseNested(key);
        if (parsed == null) {
            return getListOrEmpty(key, elementType);
        }
        else {
            return (List<E>) readObject(mapper.getTypeFactory().constructCollectionType(List.class, elementType), parsed, key);
        }
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
        JsonNode value = getNode(key);
        if (value == null) {
            throw new ConfigException("Parameter '"+key+"' is required but not set");
        }
        if (!value.isObject()) {
            throw new ConfigException("Parameter '"+key+"' must be an object");
        }
        return new Config(mapper, (ObjectNode) value);
    }

    public Config parseNested(String key)
    {
        JsonNode parsed = tryParseNested(key);
        if (parsed == null) {
            return getNested(key);
        }
        else {
            if (!parsed.isObject()) {
                throw new ConfigException("Parameter '"+key+"' must be an object");
            }
            return new Config(mapper, (ObjectNode) parsed);
        }
    }

    public Config parseNestedOrGetEmpty(String key)
    {
        JsonNode parsed = tryParseNested(key);
        if (parsed == null) {
            return getNestedOrGetEmpty(key);
        }
        else {
            if (!parsed.isObject()) {
                throw new ConfigException("Parameter '"+key+"' must be an object");
            }
            return new Config(mapper, (ObjectNode) parsed);
        }
    }

    private JsonNode tryParseNested(String key)
    {
        JsonNode node = get(key, JsonNode.class, null);
        if (node == null) {
            return null;
        }
        else if (node.isTextual()) {
            try {
                return mapper.readTree(node.textValue());
            }
            catch (IOException ex) {
                throw new ConfigException(ex);
            }
        }
        else {
            return null;
        }
    }

    public Config getNestedOrSetEmpty(String key)
    {
        JsonNode value = getNode(key);
        if (value == null || value.isNull()) {
            value = newObjectNode();
            setNode(key, value);
        }
        else if (!value.isObject()) {
            throw new ConfigException("Parameter '"+key+"' must be an object");
        }
        return new Config(mapper, (ObjectNode) value);
    }

    public Config getNestedOrGetEmpty(String key)
    {
        JsonNode value = getNode(key);
        if (value == null || value.isNull()) {
            value = newObjectNode();
        }
        else if (!value.isObject()) {
            throw new ConfigException("Parameter '"+key+"' must be an object");
        }
        return new Config(mapper, (ObjectNode) value);
    }

    public Config getNestedOrderedOrGetEmpty(String key)
    {
        JsonNode value = getNode(key);
        if (value == null) {
            value = newObjectNode();
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
            throw new ConfigException("Parameter '"+key+"' must be an object or array of objects");
        }
        return new Config(mapper, (ObjectNode) value);
    }

    public Optional<Config> getOptionalNested(String key)
    {
        JsonNode value = getNode(key);
        if (value == null || value.isNull()) {
            return Optional.absent();
        }
        return Optional.of(getNested(key));
    }

    private ObjectNode newObjectNode()
    {
        return object.objectNode();
    }

    protected JsonNode getNode(String key)
    {
        return object.get(key);
    }

    protected void setNode(String key, JsonNode value)
    {
        object.set(key, value);
    }

    private <E> E readObject(Class<E> type, JsonNode value, String key)
    {
        try {
            return mapper.readValue(value.traverse(), type);
        }
        catch (Exception ex) {
            throw propagateConvertException(ex, typeNameOf(type), value, key);
        }
    }

    private Object readObject(JavaType type, JsonNode value, String key)
    {
        try {
            return mapper.readValue(value.traverse(), type);
        }
        catch (Exception ex) {
            throw propagateConvertException(ex, typeNameOf(type), value, key);
        }
    }

    private ConfigException propagateConvertException(Exception ex, String typeName, JsonNode value, String key)
    {
        ThrowablesUtil.propagateIfInstanceOf(ex, ConfigException.class);
        String message = String.format(ENGLISH, "Expected %s for key '%s' but got %s (%s)",
                typeName, key, jsonSample(value), typeNameOf(value));
        return new ConfigException(message, ex);
    }

    private static String typeNameOf(Class<?> type)
    {
        if (type.equals(String.class)) {
            return "string type";
        }
        else if (type.equals(int.class) || type.equals(Integer.class)) {
            return "integer (int) type";
        }
        else if (type.equals(long.class) || type.equals(Long.class)) {
            return "integer (long) type";
        }
        else if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            return "'true' or 'false'";
        }
        return type.toString();
    }

    private static String typeNameOf(JavaType type)
    {
        if (List.class.isAssignableFrom(type.getRawClass())) {
            return "array type";
        }
        else if (Map.class.isAssignableFrom(type.getRawClass())) {
            return "object type";
        }
        return type.toString();
    }

    private static String typeNameOf(JsonNode value)
    {
        switch (value.getNodeType()) {
        case NULL:
            return "null";
        case BOOLEAN:
            return "boolean";
        case NUMBER:
            return "number";
        case ARRAY:
            return "array";
        case OBJECT:
            return "object";
        case STRING:
            return "string";
        case BINARY:
            return "binary";
        case POJO:
        case MISSING:
        default:
            return value.getNodeType().toString();
        }
    }

    private String jsonSample(JsonNode value)
    {
        String json = value.toString();
        if (json.length() < 100) {
            return json;
        }
        else {
            return json.substring(0, 97) + "...";
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
