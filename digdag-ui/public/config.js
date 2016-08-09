var DIGDAG_CONFIG = {
  url: 'http://localhost:65432/api/',
  navbar: {
    logo: '/logo.png',
    brand: 'Digdag',
    className: 'navbar-inverse',
    style: {
      backgroundColor: '#2B353F',
    },
  },
  auth: {
    title: 'Authentication',
    items: [
      {
        key: 'username',
        name: 'Username',
        type: 'text',
        validate: function (args) {
          if (args.value && args.value.trim()) {
            args.valid(args.key);
          } else {
            args.invalid(args.key, args.key + ' must not be empty');
          }
        },
        scrub: function (args) {
          return args.value.trim();
        }
      },
      {
        key: 'password',
        name: 'Password',
        type: 'password',
        validate: function (args) {
          if (args.value && args.value.trim()) {
            args.valid(args.key);
          } else {
            args.invalid(args.key, args.key + ' must not be empty');
          }
        },
        scrub: function (args) {
          return args.value.trim();
        }
      }
    ],
  },
  headers: function (args) {
    return {};
  }
}