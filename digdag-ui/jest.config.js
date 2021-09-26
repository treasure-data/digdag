/*
 * For a detailed explanation regarding each configuration property, visit:
 * https://jestjs.io/docs/configuration
 */

module.exports = {
  clearMocks: true,
  coverageProvider: "v8",
  setupFiles: [
    "./jest.setup.external.js",
    "./jest.setup.digdag-ui.js",
  ],
  testEnvironment: "jsdom",
};
