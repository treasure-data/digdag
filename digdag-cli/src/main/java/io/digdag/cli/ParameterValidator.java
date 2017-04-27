package io.digdag.cli;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParameterValidator implements IParameterValidator
{
    public void validate(String name, String value) throws ParameterException
    {
        if (!value.contains("=")) {
            throw new ParameterException("Parameter " + name + " expected a value of the form a=b but got:" + value);
        }
    }

    public static Map<String, String> toMap(List<String> list)
    {
        Map<String, String> map = new HashMap<>();
        String key = null;

        for (String value : list) {
            if (value.contains("=")) {
                key = value.substring(0, value.indexOf("="));
                String val = value.substring(value.indexOf("=") + 1);
                map.put(key, val);
            }
            else {
                String val = new StringBuilder(map.get(key)).append(",").append(value).toString();
                map.put(key, val);
            }
        }

        return map;
    }
}