package io.digdag.cli.client;

import java.util.List;
import java.util.Map;
import java.io.IOException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

public class ModelPrinter
{
    private static ObjectMapper buildObjectMapper()
    {
        return new ObjectMapper()
            .registerModule(new GuavaModule());
            //.registerModule(new JodaModule());
    }

    private final ObjectMapper mapper;

    public ModelPrinter()
    {
        this.mapper = buildObjectMapper();
    }

    @SuppressWarnings("unchecked")
    public void print(Object model)
        throws IOException
    {
        Map<String, Object> map = (Map<String, Object>) mapper.readValue(
                mapper.writeValueAsString(model),
                mapper.getTypeFactory()
                    .constructParametrizedType(Map.class, Map.class, String.class, Object.class));

        String format = "%" + maxKeyLength(map) + "s : %s";
        for (Map.Entry<String, Object> pair : map.entrySet()) {
            System.out.println(String.format(format, pair.getKey(), pair.getValue()));
        }
    }

    public void printList(List<?> models)
        throws IOException
    {
        for (Object model : models) {
            print(model);
            System.out.println("----");
        }
        System.out.println(String.format("%d entries.", models.size()));
    }

    private static int maxKeyLength(Map<String, Object> map)
    {
        int max = 0;
        for (String key : map.keySet()) {
            max = Math.max(max, key.length());
        }
        return max;
    }
}
