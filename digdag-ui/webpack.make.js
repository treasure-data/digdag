const MiniCssExtractPlugin = require('mini-css-extract-plugin')
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
  console.log('=== Digdag ===')
  console.log('Config: ', build ? 'Release' : 'Development')
  console.log('Build path: ', BUILD_PATH)
  console.log('Output path: ', OUTPUT_PATH)
  console.log('Timestamp:', timestamp)
  console.log('Sha:', sha)
  const config = {
    mode: build ? 'production' : 'development',
    entry: {
      tether: 'tether',
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
      rules: [{
        test: /\.(js|jsx)$/,
        exclude: /(node_modules)/,
        loader: 'babel-loader'
      }, {
        test: /\.css$/,
        use: [MiniCssExtractPlugin.loader, 'css-loader'],
        include: [path.join(__dirname, 'node_modules'), path.join(__dirname, 'public')]
      }, {
        test: /\.eot(\?v=\d+\.\d+\.\d+)?$/,
        loader: 'file-loader'
      }, {
        test: /\.(woff|woff2)$/,
        loader: 'url-loader?prefix=font/&limit=5000'
      }, {
        test: /\.ttf(\?v=\d+\.\d+\.\d+)?$/,
        loader: 'url-loader?limit=10000&mimetype=application/octet-stream'
      }, {
        test: /\.svg(\?v=\d+\.\d+\.\d+)?$/,
        loader: 'url-loader?limit=10000&mimetype=image/svg+xml'
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
        use: [MiniCssExtractPlugin.loader, 'css-loader', 'less-loader']
      }]
    },
    resolve: {
      extensions: ['*', '.js', '.jsx']
    },
    plugins: [
      // Only include the english locale for momentjs
      // This shrinks our output bundle size by a couple hundred KB
      /* eslint-disable */
      new webpack.ContextReplacementPlugin(/moment[\\\/]locale$/, /^\.\/(en)$/),
      /* eslint-enable */
      getHtmlPlugin({ build, timestamp, filename: 'index.html', sha }),
      new MiniCssExtractPlugin({
        filename: '[name].css'
      }),
      new webpack.ProvidePlugin({
        $: 'jquery',
        jQuery: 'jquery',
        'window.jQuery': 'jquery',
        'window.Tether': 'tether'
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
      new webpack.optimize.OccurrenceOrderPlugin(),
      new ManifestPlugin({ sha, timestamp })
    )
  }

  return config
}

function getHtmlPlugin ({ build, filename, sha, timestamp }) {
  const data = {
    sha,
    timestamp,
    title: process.env.CONSOLE_TITLE || 'Digdag'
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
