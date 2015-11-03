package io.digdag.core.config;

import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(as = ConfigImpl.class)
public interface Config
{
    List<String> getKeys();

    boolean isEmpty();

    boolean has(String key);

    <E> E convert(Class<E> type);

    <E> E get(String key, Class<E> type);

    Object get(String key, JavaType type);

    <E> E get(String key, TypeReference<E> type);

    <E> E get(String key, Class<E> type, E defaultValue);

    Object get(String key, JavaType type, Object defaultValue);

    <E> E get(String key, TypeReference<E> type, E defaultValue);

    <E> List<E> getList(String key, Class<E> elementType);

    <E> List<E> getListOrEmpty(String key, Class<E> elementType);

    <K, V> Map<K, V> getMap(String key, Class<K> keyType, Class<V> valueType);

    <K, V> Map<K, V> getMapOrEmpty(String key, Class<K> keyType, Class<V> valueType);

    Config getNested(String key);

    Config getNestedOrGetEmpty(String key);


    ConfigFactory getFactory();

    MutableConfig mutable();
}
