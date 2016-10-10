var fs = require('node-fs-extra')
var path = require('path')
const getSha = require('./lib/git-sha1')

try {
  const sha = process.env.SHA || getSha()
  const publicPath = path.join(__dirname, 'public')
  const buildPath = path.join(publicPath, sha)
  const config = path.join(__dirname, 'config')
  fs.copySync(path.join(buildPath, 'index.html'), path.join(publicPath, 'index.html'))
  fs.copySync(config, publicPath)
} catch (err) {
  console.error(err)
}
