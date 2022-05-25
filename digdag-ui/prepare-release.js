const fs = require('fs-extra')
const path = require('path')
const getSha = require('./lib/git-sha1')

try {
  const sha = process.env.SHA || getSha()
  const publicPath = path.join(__dirname, 'public')
  const buildPath = path.join(publicPath, sha)
  const config = path.join(__dirname, 'config')
  const imagesPath = path.join(__dirname, 'images')
  fs.copySync(path.join(buildPath, 'index.html'), path.join(publicPath, 'index.html'))
  fs.copySync(config, publicPath)
  fs.copySync(imagesPath, path.join(publicPath, 'images'))
} catch (err) {
  console.error(err)
}
