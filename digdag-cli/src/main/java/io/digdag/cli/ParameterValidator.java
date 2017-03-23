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
        list.forEach(value -> map.put(value.substring(0, value.indexOf("=")), value.substring(value.indexOf("=")+1)));
        return map;
    }
}
