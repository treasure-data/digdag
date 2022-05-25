const DIGDAG_CONFIG = {
  url: '/api/',
  td: {
    useTD: false,
    apiV4: 'https://api-console.treasuredata.com/v4',
    connectorUrl: function (connectorName) { return 'https://console.treasuredata.com/app/connections/data-transfers' },
    queryUrl: function (queryId) { return 'https://console.treasuredata.com/app/queries/' + queryId },
    jobUrl: function (jobId) { return 'https://console.treasuredata.com/app/jobs/' + jobId }
  },
  logoutUrl: '/',
  navbar: {
    logo: '/images/logo.png',
    brand: 'Digdag',
    className: 'navbar-inverse',
    style: {
      backgroundColor: '#2B353F'
    }
  },
  auth: {
    title: 'Authentication',
    items: []
  },
  headers: function (args) {
    return {}
  }
}

window.DIGDAG_CONFIG = DIGDAG_CONFIG
