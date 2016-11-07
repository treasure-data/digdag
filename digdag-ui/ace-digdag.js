// Make sure brace is loaded and global.ace is added
require('brace')

global.ace.define('ace/mode/digdag_rules', [
  'require',
  'exports',
  'module',
  'ace/lib/oop',
  'ace/mode/yaml_highlight_rules'
], function(require, exports, module) {
  const oop = require('ace/lib/oop')
  const YamlMODE = require('ace/mode/yaml_highlight_rules').YamlHighlightRules
  const DigdagRules = function() {
    this.$rules = {
      start: [
        {
          token: 'comment',
          regex: '--.*$'
        }
      ]
    }
    this.normalizeRules()
  }
  oop.inherits(DigdagRules, YamlMODE)
  exports.DigdagRules = DigdagRules
})

global.ace.define('ace/mode/digdag', [
  'require',
  'exports',
  'module',
  'ace/lib/oop',
  'ace/mode/yaml'
], function (acequire, exports, module) {
  const oop = acequire('../lib/oop')
  const YamlMode = acequire('./yaml').Mode
  const YamlHighlightRules = acequire('./digdag_rules').DigdagRules

  const Mode = function () {
    this.HighlightRules = YamlHighlightRules
  }
  oop.inherits(Mode, YamlMode)

  ; (function () {
    this.lineCommentStart = '#'
    this.$id = 'ace/mode/yaml'
  }).call(Mode.prototype)

  exports.Mode = Mode
})
