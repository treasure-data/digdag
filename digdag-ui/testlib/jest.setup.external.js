/* eslint-env jest */

jest.mock('../style.less', () => jest.fn())
jest.mock('bootstrap/scss/bootstrap.scss', () => jest.fn())

window.URL = {
  createObjectURL: () => {}
}
