const ExtractTextPlugin = require('extract-text-webpack-plugin')
const HtmlWebpackPlugin = require('html-webpack-plugin')
const path = require('path')
const webpack = require('webpack')
const ManifestPlugin = require('./lib/ManifestPlugin')
const getSha = require('./lib/git-sha1')

module.exports = function buildWebpackConfig ({ build = false }) {
  const sha = build ? (process.env.SHA || getSha()) : ''
  const BUILD_PATH = path.resolve(__dirname, 'public')
  const OUTPUT_PATH = path.join(sha, '/')
  const timestamp = new Date().toISOString()
  console.log('=== DigDag ===')
  console.log('Config: ', build ? 'Release' : 'Development')
  console.log('Build path: ', BUILD_PATH)
  console.log('Output path: ', OUTPUT_PATH)
  console.log('Timestamp:', timestamp)
  console.log('Sha:', sha)
  const config = {
    entry: {
      bootstrap: 'bootstrap-loader/extractStyles',
      app: './index.jsx'
    },
    output: {
      path: path.join(BUILD_PATH, OUTPUT_PATH),
      pathinfo: !build,
      publicPath: path.join('/', OUTPUT_PATH),
      filename: '[name].js'
    },
    target: 'web',
    devtool: build ? 'source-map' : 'cheap-module-source-map',
    module: {
      loaders: [{
        test: /\.jsx?$/,
        exclude: /(node_modules)/,
        loaders: ['babel']
      }, {
        test: /\.css$/,
        loader: ExtractTextPlugin.extract('style', 'css'),
        include: [path.join(__dirname, 'node_modules'), path.join(__dirname, 'public')]
      }, {
        test: /\.eot(\?v=\d+\.\d+\.\d+)?$/,
        loader: 'file'
      }, {
        test: /\.(woff|woff2)$/,
        loader: 'url?prefix=font/&limit=5000'
      }, {
        test: /\.ttf(\?v=\d+\.\d+\.\d+)?$/,
        loader: 'url?limit=10000&mimetype=application/octet-stream'
      }, {
        test: /\.svg(\?v=\d+\.\d+\.\d+)?$/,
        loader: 'url?limit=10000&mimetype=image/svg+xml'
      }, {
        test: /\.gif/,
        loader: 'url-loader?limit=10000&mimetype=image/gif'
      }, {
        test: /\.jpg/,
        loader: 'url-loader?limit=10000&mimetype=image/jpg'
      }, {
        test: /\.png/,
        loader: 'url-loader?limit=10000&mimetype=image/png'
      }, {
        test: /\.less$/,
        loader: ExtractTextPlugin.extract('style', 'css', 'less')
      }, {
        test: /\.json$/,
        loader: 'json-loader'
      }]
    },
    resolve: {
      extensions: ['', '.js', '.jsx']
    },
    plugins: [
      // Only include the english locale for momentjs
      // This shrinks our output bundle size by a couple hundred KB
      /* eslint-disable */
      new webpack.ContextReplacementPlugin(/moment[\\\/]locale$/, /^\.\/(en)$/),
      /* eslint-enable */
      getHtmlPlugin({ build, timestamp, filename: 'index.html', sha }),
      new ExtractTextPlugin('[name].css', {
        allChunks: true,
        disable: !build
      }),
      new webpack.ProvidePlugin({
        $: 'jquery',
        jQuery: 'jquery',
        'window.jQuery': 'jquery'
      }),
      new webpack.DefinePlugin({
        'process.env.NODE_ENV': `'${build ? 'production' : 'development'}'`
      })
    ],
    devServer: {
      historyApiFallback: {
        index: OUTPUT_PATH
      },
      contentBase: build ? BUILD_PATH : './',
      port: 9000,
      quiet: false,
      stats: {
        cached: false,
        chunk: false,
        colors: true,
        modules: false
      }
    }
  }

  // Minify, dedupe
  if (build) {
    config.plugins.push(
      new webpack.NoErrorsPlugin(),
      new webpack.optimize.DedupePlugin(),
      new webpack.optimize.OccurenceOrderPlugin(),
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
    )
  }

  return config
}

function getHtmlPlugin ({ build, filename, sha, timestamp }) {
  const data = {
    sha,
    timestamp,
    title: process.env.CONSOLE_TITLE || 'DigDag'
  }
  return new HtmlWebpackPlugin({
    build,
    data,
    filename,
    inject: false,
    minify: {
      collapseWhitespace: true,
      keepClosingSlash: true,
      minifyCSS: true,
      minifyJS: true,
      minifyURLs: true,
      removeComments: true,
      removeEmptyAttributes: true,
      removeRedundantAttributes: true,
      removeStyleLinkTypeAttributes: true,
      useShortDoctype: true
    },
    template: './lib/html-template.js'
  })
}
