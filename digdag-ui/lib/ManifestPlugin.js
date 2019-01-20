const assert = require('assert')
const _ = require('lodash')

module.exports = class ManifestPlugin {
  constructor ({ sha, timestamp }) {
    assert(sha, 'ManifestPlugin: missing SHA')
    assert(timestamp, 'ManifestPlugin: missing timestamp')
    this.data = { sha, timestamp }
  }

  apply (compiler) {
    const { data } = this

    // Add manifest.json to output
    if (compiler.hooks) {
      compiler.hooks.emit.tapAsync('ManifestPlugin', pluginEmit)
    } else {
      compiler.plugin('emit', pluginEmit)
    }

    function pluginEmit (compilation, callback) {
      const stats = compilation.getStats().toJson({
        assets: true,
        children: false,
        chunks: false,
        errorDetails: false,
        errors: false,
        hash: true,
        modules: false,
        publicPath: true,
        source: false,
        timings: false,
        version: false,
        warnings: false
      })
      const manifest = _.pick(stats, [
        'assets',
        'assetsByChunkName',
        'hash',
        'publicPath'
      ])
      Object.assign(manifest, data)
      const manifestSource = JSON.stringify(manifest)
      compilation.assets['manifest.json'] = {
        size: () => manifestSource.length,
        source: () => manifestSource
      }

      callback()
    }
  }
}
