const child = require('child_process')
const path = require('path')

module.exports = function getSHA1 () {
  return child.execSync('git rev-parse --short HEAD', {
    cwd: path.resolve(__dirname, '..'),
    encoding: 'utf8'
  }).trim()
}
