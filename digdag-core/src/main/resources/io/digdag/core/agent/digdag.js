function template(input, variables, extendedSyntax)
{
  var escapes = {
    "'":      "'",
    '\\':     '\\',
    '\r':     'r',
    '\n':     'n',
    '\u2028': 'u2028',
    '\u2029': 'u2029'
  };

  var escaper = /\\|'|\r|\n|\u2028|\u2029/g;

  var escapeChar = function(match) {
    return '\\' + escapes[match];
  };

  if (extendedSyntax) {
    // (?![a-z]+:) => exclude operator-defined templates such as ${secret:sec.ret.key}
    var matcher = /\$\{(?![a-z]+:)([\s\S]+)|$/;

    var position = 0;
    var source = "__p+='";
    while (position < input.length) {
      m = input.slice(position).match(matcher);
      if (m) {
        all = m[0];
        match = m[1];
        // append text before '${' - THIS_TEXT${...}
        source += input.slice(position, position + m.index).replace(/\$\$/g, "$").replace(escaper, escapeChar);
        position += m.index;

        if (match) {
          // for the text after '${' - ${THIS_TEXT}
          var paren = 0;
          var script = "";
          for (var i = 0; i < match.length; i++) {
            if (match[i] == '{') {
              paren++;
            } else if (match[i] == '}') {
              paren--;
              if (paren == -1) {
                script = match.slice(0, i);
                break;
              }
            }
          }

          if (paren != -1) {
            source += all.replace(/\$\$/g, "$").replace(escaper, escapeChar);
            position += all.length;
          } else {
            if (script) {
              source += "'+\n((__t=(" + script + "))==null?'':(typeof __t==\"string\"?__t:JSON.stringify(__t)))+\n'";
            }
            position += script.length + (all.length - match.length + 1);
          }
        }
      }
    }

  } else {
    // (?![a-z]+:) => exclude operator-defined templates such as ${secret:sec.ret.key}
    var matcher = /\${(?![a-z]+:)((?:\{)|[\s\S]+?)}|$/g;

    var index = 0;
    var source = "__p+='";
    input.replace(matcher, function(match, expression, offset) {
      source += input.slice(index, offset).replace(/\$\$/g, "$").replace(escaper, escapeChar);
      index = offset + match.length;

      if (expression) {
        source += "'+\n((__t=(" + expression + "))==null?'':(typeof __t==\"string\"?__t:JSON.stringify(__t)))+\n'";
      }

      return match;
    });
  }

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
