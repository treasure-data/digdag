package io.digdag.core.yaml;

import com.hubspot.jinjava.tree.TagNode;
import com.hubspot.jinjava.lib.tag.IncludeTag;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;

public class JinjaLoadTag
    extends IncludeTag
{
    @Override
    public String interpret(TagNode tagNode, JinjavaInterpreter interpreter)
    {
        String yamlContent = super.interpret(tagNode, interpreter);
        return JinjaYamlExpressions.parse(yamlContent);
    }
}
