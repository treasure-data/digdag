/*
 * For a detailed explanation regarding each configuration property, visit:
 * https://jestjs.io/docs/configuration
 */

module.exports = {
  clearMocks: true,
  coverageProvider: 'v8',
  setupFiles: [
    './testlib/jest.setup.external.js',
    './testlib/jest.setup.fetch.js',
    './testlib/jest.setup.digdag-ui.js'
  ],
  testEnvironment: 'jsdom'
}
