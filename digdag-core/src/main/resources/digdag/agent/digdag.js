function template(code, variables)
{
  var matcher = RegExp([
    (/\${%([\s\S]+?)%}/g).source,
    (/\${([\s\S]+?)}/g).source
  ].join('|') + '|$', 'g');

  var index = 0;
  var source = "__p+='";
  code.replace(matcher, function(match, statement, expression, offset) {
    source += code.slice(index, offset).replace(/\$\$/g, "$").replace(/\\|'/g, function(c) { return '\\' + c });
    index = offset + match.length;

    if (statement) {
      source += "'+\n((__t=(" + statement + "))==null?'':__t)+\n'";
    } else if (expression) {
      source += "'+\n((__t=(" + expression + "))==null?'':(typeof __t==\"string\"?__t:JSON.stringify(__t)))+\n'";
    }

    return match;
  });
  source += "';\n";

  source = "var __t,__p='',__j=Array.prototype.join," +
    "print=function(){__p+=__j.call(arguments,'');};\n" +
    source + 'return __p;\n';

  source = 'with(this){\n' + source + '}\n';

  try {
    var func = new Function(source);
  } catch (e) {
    e.source = source;
    throw e;
  }

  if (typeof variables == "string") {
    variables = JSON.parse(variables);
  }
  vs = func.call(variables);

  return vs;
}
