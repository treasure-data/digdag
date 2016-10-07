import React from 'react'
import ReactDOM from 'react-dom'
import Console from './console'

console.log('f', process.env)
ReactDOM.render(
  <Console />,
  document.getElementById('app')
)
