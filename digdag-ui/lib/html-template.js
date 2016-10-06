module.exports = function htmlTemplate ({ htmlWebpackPlugin }) {
  const { files, options: { build, data } } = htmlWebpackPlugin
  return `
<!DOCTYPE html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <link href='${data.API_ROOT}' rel='preconnect' crossorigin>
  <link href='${data.AUTH_ROOT}' rel='dns-prefetch'>
  <meta name='viewport'
    content='width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no'>
  <title>Loading...</title>
</head>
<body>
  <div id='app'></div>
  <script>
    window.DIGDAG_CONFIG = ${JSON.stringify(data)};
  </script>
  <script src='${files.chunks.bootstrap.entry}'></script>
  <script src='${files.chunks.app.entry}'></script>
  ${[...files.css].map((css) => `
    <link href='${css}' rel='stylesheet'>
  `).join('')}
</body>
</html>
`
}
