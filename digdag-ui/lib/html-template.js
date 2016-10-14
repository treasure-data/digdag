module.exports = function htmlTemplate ({ htmlWebpackPlugin }) {
  const { files, options: { build, data } } = htmlWebpackPlugin
  return `
    <!DOCTYPE html>
    <html lang='en'>
    <head>
      <meta charset='utf-8'>
      <meta name='viewport' content='width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no'>
      <title>${data.title}</title>
    </head>
    <body>
      <div id='app'></div>
      <script src='/config.js' type='text/javascript'></script>
      <script src='${files.chunks.bootstrap.entry}'></script>
      <script src='${files.chunks.app.entry}'></script>
      ${[...files.css].map((css) => `
        <link href='${css}' rel='stylesheet'>
      `).join('')}
    </body>
    </html>
    `
}
