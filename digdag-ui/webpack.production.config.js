var webpack = require('webpack')
var path = require('path')
var loaders = require('./webpack.loaders')
var ManifestPlugin = require('./lib/ManifestPlugin')
var getSHA1 = require('./lib/git-sha1')

const timestamp = new Date().toISOString()
const sha = process.env.SHA || getSHA1()

module.exports = {
  devtool: 'source-map',
  entry: [
    './index.jsx'
  ],
  output: {
    path: path.join(__dirname, 'public'),
    filename: 'bundle.js'
  },
  resolve: {
    extensions: ['', '.js', '.jsx']
  },
  module: {
    loaders: loaders
  },
  plugins: [
    new webpack.NoErrorsPlugin(),
    new webpack.optimize.DedupePlugin(),
    new webpack.optimize.OccurenceOrderPlugin(),
    new webpack.ProvidePlugin({
      $: 'jquery',
      jQuery: 'jquery',
      'window.jQuery': 'jquery'
    }),
    new webpack.DefinePlugin({
      'process.env': {
        'NODE_ENV': JSON.stringify('production')
      }
    }),
    new webpack.optimize.UglifyJsPlugin({
      compress: {
        warnings: false
      }
    }),
    new ManifestPlugin({ sha, timestamp })
  ]
}
