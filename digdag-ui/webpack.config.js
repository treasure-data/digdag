var webpack = require('webpack')
var path = require('path')
var loaders = require('./webpack.loaders')

var FlowStatusWebpackPlugin = require('flow-status-webpack-plugin')

module.exports = {
  entry: [
    'babel-polyfill',
    'whatwg-fetch',
    'webpack-dev-server/client?http://0.0.0.0:9000', // WebpackDevServer host and port
    'webpack/hot/only-dev-server',
    'bootstrap-loader',
    './index.jsx'
  ],
  output: {
    path: path.join(__dirname, 'public'),
    filename: 'bundle.js',
    publicPath: '/'
  },
  devtool: process.env.WEBPACK_DEVTOOL || 'source-map',
  resolve: {
    extensions: ['', '.js', '.jsx']
  },
  module: {
    loaders: loaders
  },
  devServer: {
    contentBase: './public',
    noInfo: true, //  --no-info option
    hot: true,
    inline: true,
    historyApiFallback: true
  },
  plugins: [
    new webpack.NoErrorsPlugin(),
    new webpack.ProvidePlugin({
      $: 'jquery',
      jQuery: 'jquery',
      'window.jQuery': 'jquery'
    }),
    new FlowStatusWebpackPlugin()
  ]
}
