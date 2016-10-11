function readCookie(name) {
  var cookies = document.cookie.split(';').reduce(function (cookies, cookieString) {
    var cookie = cookieString.split('=');
    cookies[cookie[0].trim()] = cookie[1];
    return cookies;
  }, {});
  return cookies[name];
}

var DIGDAG_CONFIG = {
  url: 'https://api-development-workflow.treasuredata.com/api/',
  td: {
    apiV4: 'https://api-development-console.treasuredata.com/v4',
    connectorUrl: function (connectorName) { return 'https://console-development.treasuredata.com/connections/data-transfers'},
    queryUrl: function (queryId) { return 'https://console-development.treasuredata.com/queries/' + queryId; },
    jobUrl: function (jobId) { return 'https://console-development.treasuredata.com/jobs/' + jobId; }
  },
  logoutUrl: 'https://workflows-development.treasuredata.com/users/sign_out',
  navbar: {
    logo: '/logo.png',
    brand: 'Treasure Workflow',
  },
  auth: {
    title: 'Authentication',
    items: [],
  },
  headers: function(args) {
    if (!document.cookie) {
      return {};
    }
    var headers = {};
    if (window.sessionStorage) {
      var accountOverride = window.sessionStorage.getItem('td.account-override');
      if (accountOverride) {
        console.log('Using TD Account override:', accountOverride)
        headers['X-TD-Account-Override'] = accountOverride;
      }
    }
    headers['X-XSRF-TOKEN'] = readCookie('XSRF-TOKEN-DEVELOPMENT');
    return headers;
  }
}

function DIGDAG_OVERRIDE_ACCOUNT(accountId) {
  if (!window.sessionStorage) {
    console.log("Please use a browser with session storage support");
    return;
  }
  if (!accountId) {
    window.sessionStorage.removeItem('td.account-override');
    console.log('TD Account override disabled');
  } else {
    window.sessionStorage.setItem('td.account-override', accountId);
    console.log('TD Account override set to:', accountId);
  }
  location.reload();
}
