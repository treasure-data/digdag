jest.mock('')
import './config/config'
import {
  setup as setupModel
} from './model'

setupModel({
  url: DIGDAG_CONFIG.url,
  td: DIGDAG_CONFIG.td,
  credentials: null,
  headers: DIGDAG_CONFIG.headers
})