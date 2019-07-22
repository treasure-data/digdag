// @flow
import './style.less'

import '@babel/polyfill'
import 'whatwg-fetch'

import 'bootstrap/dist/js/bootstrap'

import _ from 'lodash'

import React from 'react'
import PropTypes from 'prop-types'
import {
  BrowserRouter as Router,
  Route,
  Link,
  Switch,
  withRouter,
  matchPath
} from 'react-router-dom'
import Measure from 'react-measure'
import Tar from 'tar-js'
import moment from 'moment'
import 'moment-duration-format'
import pako from 'pako'
import path from 'path'
import yaml from 'js-yaml'
import Duration from 'duration'
import uuidv4 from 'uuid/v4'
import jQuery from 'jquery'
import ReactInterval from 'react-interval'
import { Buffer } from 'buffer/'
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome'
import {
  faSignOutAlt,
  faCheckCircle,
  faExclamationCircle,
  faPlayCircle,
  faSyncAlt
} from '@fortawesome/free-solid-svg-icons'

// noinspection ES6UnusedImports
import { TD_LOAD_VALUE_TOKEN, TD_RUN_VALUE_TOKEN } from './ace-digdag'

/* eslint-disable */
// see https://github.com/gajus/eslint-plugin-flowtype/issues/72
import type {
  Attempt,
  Credentials,
  HeadersProvider,
  LogFileHandle,
  NameOptionalId,
  Project,
  ProjectArchive,
  Session,
  TarEntry,
  Task,
  Workflow
} from './model'

import {
  model,
  setup as setupModel
} from './model'
/* eslint-enable */

type Scrubber = (args:{key: string, value: string}) => string

const refreshIntervalMillis = 5000

const isDevelopmentEnv = process.env.NODE_ENV !== 'production'

type AuthItem = {
  key: string;
  name: string;
  type: string;
  validate: (args:{
    key: string;
    value: string;
    valid: (key:string) => void;
    invalid: (key:string) => void;
  }) => void;
  scrub: Scrubber;
}

/* eslint-disable */
// because of the global type, this results in an eslint error (when it's not)
type ConsoleConfig = {
  url: string;
  td: {
    useTD: boolean;
    apiV4: string,
    jobUrl: (id:string) => string;
    connectorUrl: (id:string) => string;
    queryUrl: (id:string) => string;
  },
  logoutUrl: ?string,
  navbar: ?{
    className: ?string;
    brand: ?string;
    logo: ?string;
    style: ?Object;
  };
  auth: {
    title: string;
    items: Array<AuthItem>;
  };
  headers: HeadersProvider;
}

declare var DIGDAG_CONFIG:ConsoleConfig;
/* eslint-enable */

function MaybeWorkflowLink ({ workflow } : { workflow: NameOptionalId }) {
  if (workflow.id) {
    return <Link to={`/workflows/${workflow.id}`}>{workflow.name}</Link>
  }
  return <span>{workflow.name}</span>
}

class CodeViewer extends React.Component {
  props: {
    className: ?string;
    language: string;
    value: string;
    editorOptions: Object;
  }

  static defaultProps = {
    editorOptions: {
      enableLinking: true,
      highlightActiveLine: false,
      readOnly: true,
      showLineNumbers: false,
      showGutter: false,
      showPrintMargin: false,
      tabSize: 2,
      useSoftTabs: true,
      maxLines: Infinity
    }
  }

  /* eslint-disable */
  editor: HTMLElement; // standard does not have definition of flow.dom.HTMLElement
  /* eslint-enable */

  _editor: any; // we have no definition for AceEditor

  constructor (props) {
    super(props)
    // Uses ace 1.1.9.
    const Ace = require('brace')
    require('brace/ext/language_tools')
    require('brace/ext/linking')
    require('brace/mode/json')
    require('brace/mode/sql')
    require('brace/mode/yaml')
    require('brace/mode/python')
    require('brace/mode/scala')
    Ace.acequire('ace/ext/language_tools')
    Ace.acequire('ace/ext/linking')
  }

  getValue () {
    if (this._editor) {
      return this._editor.getValue()
    } else {
      return this.props.value
    }
  }

  componentDidMount () {
    const Ace = require('brace')
    require('./ace-digdag')
    this._editor = Ace.edit(this.editor)
    this._editor.setOptions(this.props.editorOptions)
    this._editor.$blockScrolling = Infinity
    this._editor.on('click', this.editorClick.bind(this))
    this._updateEditor(this.props)
  }

  editorClick (event) {
    const editor = event.editor
    const docPos = event.getDocumentPosition()
    const session = editor.session
    const token = session.getTokenAt(docPos.row, docPos.column)
    const openTab = (url) => window.open(url)
    if (token && token.type === TD_LOAD_VALUE_TOKEN) {
      const queryId = model().getTDQueryIdFromName(token.valuke)
      if (queryId) {
        openTab(DIGDAG_CONFIG.td.queryUrl(queryId))
      }
    } else if (token && token.type === TD_RUN_VALUE_TOKEN) {
      openTab(DIGDAG_CONFIG.td.connectorUrl(token.value))
    }
  }

  componentWillReceiveProps (nextProps) {
    if (nextProps.value !== this.props.value) {
      this._updateEditor(nextProps)
    }
  }

  componentWillUnmount () {
    this._editor.destroy()
  }

  _updateEditor (props) {
    const DOCUMENT_END = 1
    const {
      language,
      value
    } = props
    this._editor.setValue(value, DOCUMENT_END)
    this._editor.session.setMode(`ace/mode/${language}`)
    this._editor.resize(true)
  }

  render () {
    const { className } = this.props
    return (
      <div
        className={className}
        key='editor'
        ref={(value) => { this.editor = value }}
      />
    )
  }
}

class CodeEditor extends React.Component {
  _editor: any;

  render () {
    const editorOptions = {
      enableLinking: false,
      highlightActiveLine: true,
      readOnly: false,
      showGutter: true,
      showLineNumbers: true
    }
    return (
      <CodeViewer editorOptions={editorOptions} ref={(value) => { this._editor = value }} {...this.props} />
    )
  }

  getValue () {
    return this._editor && this._editor.getValue()
  }
}

class CacheLoader extends React.Component {
  state = {
    hasCache: false
  };

  componentWillMount () {
    model().fillTDQueryCache().then(() => {
      this.setState({ hasCache: true })
    })
  }

  render () {
    const { hasCache } = this.state
    const { children } = this.props
    if (!hasCache) {
      return (
        <div className='loading-container'>
          <span className='spinner-border' role='status' />
          <span className='loading-text'>Loading...</span>
        </div>
      )
    }
    return children
  }
}

class ProjectListView extends React.Component {
  props:{
    projects: Array<Project>;
  };

  render () {
    const projectRows = this.props.projects.map(project =>
      <tr key={project.id}>
        <td><Link to={`/projects/${project.id}`}>{project.name}</Link></td>
        <td><Timestamp t={project.updatedAt} /></td>
        <td>{project.revision}</td>
      </tr>
    )
    return (
      <div className='table-responsive'>
        <table className='table table-striped table-hover table-sm'>
          <thead>
            <tr>
              <th>Name</th>
              <th>Updated</th>
              <th>Revision</th>
            </tr>
          </thead>
          <tbody>
            {projectRows}
          </tbody>
        </table>
      </div>
    )
  }
}

class WorkflowListView extends React.Component {
  props:{
    workflows: Array<Workflow>;
  };

  render () {
    const rows = this.props.workflows.map(workflow =>
      <tr key={workflow.id}>
        <td><Link
          to={`/projects/${workflow.project.id}/workflows/${encodeURIComponent(workflow.name)}`}>{workflow.name}</Link>
        </td>
        <td>{workflow.revision}</td>
        <td>{workflow.project.name}</td>
      </tr>
    )
    return (
      <div className='table-responsive'>
        <table className='table table-striped table-hover table-sm'>
          <thead>
            <tr>
              <th>Name</th>
              <th>Revision</th>
              <th>Project</th>
            </tr>
          </thead>
          <tbody>
            {rows}
          </tbody>
        </table>
      </div>
    )
  }
}

const AttemptStatusView = ({ attempt }) => {
  if (attempt.done) {
    if (attempt.success) {
      return <span><FontAwesomeIcon icon={faCheckCircle} className='text-success' /> Success</span>
    } else if (attempt.cancelRequested) {
      return <span><FontAwesomeIcon icon={faExclamationCircle} className='text-warning' /> Canceled</span>
    } else {
      return <span><FontAwesomeIcon icon={faExclamationCircle} className='text-danger' /> Failure</span>
    }
  } else {
    if (attempt.cancelRequested) {
      return <span><FontAwesomeIcon icon={faExclamationCircle} className='text-warning' /> Canceling</span>
    } else {
      return <span><FontAwesomeIcon icon={faSyncAlt} className='text-info' /> Pending</span>
    }
  }
}

function attemptCanRetryAll (attempt) {
  if (!attempt) {
    return false
  }
  return attempt.done || attempt.cancelRequested
}

function attemptCanResume (attempt) {
  if (!attempt) {
    return false
  }
  return attempt.done && !attempt.success
}

function attemptCanBeKilled (attempt) {
  if (!attempt) {
    return false
  }
  return !attempt.done && !attempt.cancelRequested
}

const SessionStatusView = ({ session }:{session: Session}) => {
  const attempt = session.lastAttempt
  return attempt
    ? <Link to={`/attempts/${attempt.id}`}><AttemptStatusView attempt={attempt} /></Link>
    : <span><FontAwesomeIcon icon={faSyncAlt} className='text-info' /> Pending</span>
}

SessionStatusView.propTypes = {
  session: PropTypes.object.isRequired
}

class SessionRevisionView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    session: Session;
  };

  state = {
    workflow: null
  };

  componentDidMount () {
    this.fetchWorkflow()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetchWorkflow()
  }

  fetchWorkflow () {
    const id = this.props.session.workflow.id
    if (!id) {
      return
    }
    model().fetchWorkflow(id).then(workflow => {
      if (!this.ignoreLastFetch) {
        this.setState({ workflow })
      }
    })
  }

  render () {
    return this.state.workflow
      ? <span>{this.state.workflow.revision}</span>
      : <span />
  }
}

class AttemptListView extends React.Component {
  props:{
    attempts: Array<Attempt>;
  };

  render () {
    const rows = this.props.attempts.map(attempt => {
      return (
        <tr key={attempt.id}>
          <td><Link to={`/attempts/${attempt.id}`}>{attempt.id}</Link></td>
          <td><MaybeWorkflowLink workflow={attempt.workflow} /></td>
          <td><Timestamp t={attempt.createdAt} /></td>
          <td><SessionTime t={attempt.sessionTime} /></td>
          <td><DurationView start={attempt.createdAt} end={attempt.finishedAt} /></td>
          <td><AttemptStatusView attempt={attempt} /></td>
        </tr>
      )
    })

    return (
      <div>
        <h2>Attempts</h2>
        <div className='table-responsive'>
          <table className='table table-striped table-hover table-sm'>
            <thead>
              <tr>
                <th>ID</th>
                <th>Workflow</th>
                <th>Created</th>
                <th>Session Time</th>
                <th>Duration</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {rows}
            </tbody>
          </table>
        </div>
      </div>
    )
  }
}

class SessionListView extends React.Component {
  props:{
    sessions: Array<Session>;
  };

  render () {
    const rows = this.props.sessions.map(session => {
      const lastAttemptCreatedAt = session.lastAttempt ? session.lastAttempt.createdAt : null
      const lastAttemptFinishedAt = session.lastAttempt ? session.lastAttempt.finishedAt : null
      return (
        <tr key={session.id}>
          <td><Link to={`/sessions/${session.id}`}>{session.id}</Link></td>
          <td><Link to={`/projects/${session.project.id}`}>{session.project.name}</Link></td>
          <td><MaybeWorkflowLink workflow={session.workflow} /></td>
          <td><SessionRevisionView session={session} /></td>
          <td><SessionTime t={session.sessionTime} /></td>
          <td><Timestamp t={lastAttemptCreatedAt} /></td>
          <td><DurationView start={lastAttemptCreatedAt} end={lastAttemptFinishedAt} /></td>
          <td><SessionStatusView session={session} /></td>
        </tr>
      )
    })

    return (
      <div className='table-responsive'>
        <table className='table table-striped table-hover table-sm'>
          <thead>
            <tr>
              <th>ID</th>
              <th>Project</th>
              <th>Workflow</th>
              <th>Revision</th>
              <th>Session Time</th>
              <th>Last Attempt</th>
              <th>Last Attempt Duration</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {rows}
          </tbody>
        </table>
      </div>
    )
  }
}

class ScheduleListView extends React.Component {
  props:{
    workflowName: string;
    projectId: string;
  };

  state: {
    schedules: []
  };

  constructor (props) {
    super(props)
    this.state = {
      schedules: []
    }
  }

  componentDidMount () {
    this.fetch()
  }

  fetch () {
    model().fetchProjectWorkflowSchedule(this.props.projectId, this.props.workflowName).then(({ schedules }) => {
      this.setState({ schedules })
    })
  }

  disableSchedule () {
    const { schedules } = this.state
    if (!schedules || !schedules.length) {
      return
    }
    model()
      .disableSchedule(_.get(schedules, ['0', 'id']))
      .then(() => this.fetch())
  }

  enableSchedule () {
    const { schedules } = this.state
    if (!schedules || !schedules.length) {
      return
    }
    model()
      .enableSchedule(_.get(schedules, ['0', 'id']))
      .then(() => this.fetch())
  }

  render () {
    const { schedules } = this.state
    const hasSchedule = schedules && schedules.length
    const isPaused = hasSchedule && !_.get(schedules, ['0', 'disabledAt'])
    const rows = (schedules || []).map(schedule => {
      return (
        <tr key={schedule.id}>
          <td>{schedule.id}</td>
          <td>{schedule.revision}</td>
          <td><Link to={`/projects/${schedule.project.id}`}>{schedule.project.name}</Link></td>
          <td><Link to={`/workflows/${schedule.workflow.id}`}>{schedule.workflow.name}</Link></td>
          <td><FullTimestamp showAgo={Boolean(true)} t={schedule.nextRunTime} /></td>
          <td><SessionTime t={schedule.nextScheduleTime} /></td>
        </tr>
      )
    })
    const statusButton = isPaused ? (
      <button
        className='btn btn-sm btn-secondary float-right'
        onClick={this.disableSchedule.bind(this)}
      >
        PAUSE
      </button>
    ) : (
      <button
        className='btn btn-sm btn-success float-right'
        onClick={this.enableSchedule.bind(this)}
      >
        RESUME
      </button>
    )
    return (
      <div>
        <h2 className='d-inline-flex'>
          Scheduling
        </h2>
        {hasSchedule ? statusButton : ''}
        <div className='table-responsive'>
          <table className='table table-striped table-hover table-sm'>
            <thead>
              <tr>
                <th>ID</th>
                <th>Revision</th>
                <th>Project</th>
                <th>Workflow</th>
                <th>Next Run Time</th>
                <th>Next Schedule Time</th>
              </tr>
            </thead>
            <tbody>
              {rows}
            </tbody>
          </table>
        </div>
        <ReactInterval timeout={refreshIntervalMillis} enabled={Boolean(true)} callback={() => this.fetch()} />
      </div>
    )
  }
}

class ProjectsView extends React.Component {
  state = {
    projects: []
  };

  componentDidMount () {
    this.fetch()
  }

  fetch () {
    model().fetchProjects().then(({ projects }) => {
      this.setState({ projects })
    })
  }

  render () {
    return (
      <div className='projects'>
        <h2>Projects</h2>
        <ProjectListView projects={this.state.projects} />
        <ReactInterval timeout={refreshIntervalMillis} enabled={Boolean(true)} callback={() => this.fetch()} />
      </div>
    )
  }
}

class StatusFilter extends React.Component {
  static Status = class Status {
    static ALL = 'All';
    static SUCCESS = 'Success';
    static FAILURE = 'Failure';
    static PENDING = 'Pending';
    static CANCELED = 'Canceled';
    static CANCELING = 'Canceling';

    static allStatus () {
      return [Status.ALL, Status.SUCCESS, Status.FAILURE, Status.PENDING, Status.CANCELED, Status.CANCELING]
    }
  };

  state = {
    selectedStatus: StatusFilter.Status.ALL
  };

  filterSessionsByStatus (sessions, selectedStatus) {
    switch (selectedStatus) {
      case StatusFilter.Status.SUCCESS:
        return sessions.filter(s => s.lastAttempt.done && s.lastAttempt.success)
      case StatusFilter.Status.FAILURE:
        return sessions.filter(s => s.lastAttempt.done && !s.lastAttempt.success && !s.lastAttempt.cancelRequested)
      case StatusFilter.Status.PENDING:
        return sessions.filter(s => !s.lastAttempt.done && !s.lastAttempt.cancelRequested)
      case StatusFilter.Status.CANCELED:
        return sessions.filter(s => s.lastAttempt.cancelRequested && s.lastAttempt.done)
      case StatusFilter.Status.CANCELING:
        return sessions.filter(s => s.lastAttempt.cancelRequested && !s.lastAttempt.done)
      default:
        return sessions
    }
  }

  render () {
    const childrenWithProps = React.Children.map(this.props.children, child => {
      return React.cloneElement(child, {
        sessions: this.filterSessionsByStatus(this.props.sessions, this.state.selectedStatus)
      })
    })

    return (
      <div className='status-filter'>
        <form className='form-inline mb-2'>
          <label className='control-label pr-2'>Status:</label>
          <select className='form-control form-control-sm' onChange={(e) => this.setState({ selectedStatus: e.target.value })}>
            {StatusFilter.Status.allStatus().map(s => <option key={s} value={s}>{s}</option>)}
          </select>
        </form>
        {childrenWithProps}
      </div>
    )
  }
}

class SessionsView extends React.Component {
  state = {
    sessions: []
  };

  componentDidMount () {
    this.fetch()
  }

  fetch () {
    model().fetchSessions().then(({ sessions }) => {
      this.setState({ sessions })
    })
  }

  render () {
    return (
      <div>
        <h2>Sessions</h2>
        <StatusFilter sessions={this.state.sessions} >
          <SessionListView />
        </StatusFilter>
        <ReactInterval timeout={refreshIntervalMillis} enabled={Boolean(true)} callback={() => this.fetch()} />
      </div>
    )
  }
}

class ProjectView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    projectId: string;
  };

  state = {
    project: {},
    workflows: [],
    sessions: []
  };

  componentDidMount () {
    this.fetch()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetch()
  }

  fetch () {
    model().fetchProject(this.props.projectId).then(project => {
      if (!this.ignoreLastFetch) {
        this.setState({ project: project })
      }
      return project
    }).then(project => {
      if (!this.ignoreLastFetch) {
        model().fetchProjectSessions(project.id).then(({ sessions }) => {
          if (!this.ignoreLastFetch) {
            this.setState({ sessions })
          }
        })
      }
    })
    model().fetchProjectWorkflows(this.props.projectId).then(({ workflows }) => {
      if (!this.ignoreLastFetch) {
        this.setState({ workflows })
      }
    })
  }

  render () {
    const project = this.state.project
    return (
      <div>
        <div>
          <h2>Project</h2>
          <table className='table table-sm'>
            <tbody>
              <tr>
                <td>ID</td>
                <td>{project.id}</td>
              </tr>
              <tr>
                <td>Name</td>
                <td>{project.name}</td>
              </tr>
              <tr>
                <td>Revision</td>
                <td>{project.revision}</td>
              </tr>
              <tr>
                <td>Created</td>
                <td><FullTimestamp showAgo={Boolean(true)} t={project.createdAt} /></td>
              </tr>
              <tr>
                <td>Updated</td>
                <td><FullTimestamp showAgo={Boolean(true)} t={project.updatedAt} /></td>
              </tr>
            </tbody>
          </table>
        </div>
        <div>
          <h2 className='d-inline-flex'>Workflows</h2>
          <div className='float-right'>
            <Link
              className='btn btn-sm btn-outline-secondary'
              to={`/projects/${project.id}/edit`}
            >
              Edit workflows
            </Link>
          </div>
          <WorkflowListView workflows={this.state.workflows} />
        </div>
        <div>
          <h2>Sessions</h2>
          <StatusFilter sessions={this.state.sessions}>
            <SessionListView />
          </StatusFilter>
        </div>
        <ReactInterval timeout={refreshIntervalMillis} enabled={Boolean(true)} callback={() => this.fetch()} />
      </div>
    )
  }
}

class WorkflowView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    workflow: Workflow;
  };

  state = {
    sessions: [],
    projectArchive: null
  };

  componentDidMount () {
    this.fetch()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetch()
  }

  fetch () {
    model().fetchProjectWorkflowSessions(this.props.workflow.project.id, this.props.workflow.name).then(({ sessions }) => {
      if (!this.ignoreLastFetch) {
        this.setState({ sessions })
      }
    })
    model().fetchProjectArchiveWithRevision(this.props.workflow.project.id, this.props.workflow.revision).then(projectArchive => {
      if (!this.ignoreLastFetch) {
        this.setState({ projectArchive })
      }
    })
  }

  definition () {
    if (!this.state.projectArchive) {
      return ''
    }
    const workflow = this.state.projectArchive.getWorkflow(this.props.workflow.name)
    if (!workflow) {
      return ''
    }
    return workflow.trim()
  }

  runWorkflow () {
    const sessionTime = moment().format()
    const params = {}
    model().startAttempt(this.props.workflow.id, sessionTime, params)
  }

  render () {
    const wf = this.props.workflow
    return (
      <div>
        <div>
          <h2 className='d-inline-flex'>Workflow</h2>
          <button
            className='btn btn-sm btn-success float-right'
            onClick={this.runWorkflow.bind(this)}
          >
            RUN
          </button>
          <table className='table table-sm'>
            <tbody>
              <tr>
                <td>ID</td>
                <td>{wf.id}</td>
              </tr>
              <tr>
                <td>Name</td>
                <td>{wf.name}</td>
              </tr>
              <tr>
                <td>Project</td>
                <td><Link to={`/projects/${wf.project.id}`}>{wf.project.name}</Link></td>
              </tr>
              <tr>
                <td>Revision</td>
                <td>{wf.revision}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <ScheduleListView workflowName={wf.name} projectId={wf.project.id} />
        <div>
          <h2 className='d-inline-flex'>Definition</h2>
          <button className='btn btn-sm btn-light float-right'><Link to={`/projects/${wf.project.id}/edit`}>Edit</Link></button>
          <div className='card bg-light my-2 pre-scrollable'>
            <div className='card-body p-2'>
              <pre className='mb-0'>
                <code>
                  <Measure>
                    { ({ width }) =>
                      <CodeViewer
                        className='definition'
                        language='digdag'
                        value={this.definition()}
                        style={{ width }}
                      />
                    }
                  </Measure>
                </code>
              </pre>
            </div>
          </div>
        </div>
        <div>
          <h2>Sessions</h2>
          <StatusFilter sessions={this.state.sessions}>
            <SessionListView />
          </StatusFilter>
        </div>
        <div>
          <h2>Files</h2>
          <WorkflowFilesView workflow={wf} projectArchive={this.state.projectArchive} />
        </div>
        <ReactInterval timeout={refreshIntervalMillis} enabled={Boolean(true)} callback={() => this.fetch()} />
      </div>
    )
  }
}

type TaskFile = {
  name: string;
  taskType: string;
  fileType: string;
};

function task (node:Object) {
  let command = ''
  let taskType = node['_type'] || ''
  if (taskType) {
    command = node['_command']
  } else {
    const operators = ['td', 'td_load', 'sh', 'rb', 'py', 'mail', 'redshift']
    for (let operator of operators) {
      command = node[operator + '>'] || ''
      if (command) {
        taskType = operator
        break
      }
    }
    const externalOperators = ['spark', 'script']
    if (!taskType && externalOperators.includes(node.type)) {
      // external operators
      taskType = node.type
      command = node.application
    }
  }
  return { taskType, command }
}

function resolveTaskFile (taskType:string, command:string, task:Object, projectArchive:ProjectArchive):?TaskFile {
  // TODO: resolve paths relative from the workflow file
  // TODO: make operators provide information about files used in a structured way instead of this hack
  let filename = ''
  const cmd = path.normalize(command)
  _.forEach(projectArchive.files, function (p) {
    // check the cmd contains filename
    if (cmd.indexOf(p.name) !== -1) {
      filename = p.name
    }
  })
  if (!projectArchive.hasFile(filename)) {
    return null
  }
  const fileTypes = {
    'td': 'sql',
    'td_load': 'yaml',
    'sh': 'bash',
    'py': 'python',
    'rb': 'ruby',
    'spark': 'python',
    'redshift': 'sql',
    'mail': task['html'] ? 'html' : 'txt'
  }
  const fileType = fileTypes[taskType]
  if (!fileType) {
    return null
  }
  return { taskType, name: filename, fileType }
}

function enumerateTaskFiles (node:Object, files:Array<TaskFile>, projectArchive:ProjectArchive) {
  if (node && typeof node === 'object') {
    let { taskType, command } = task(node)
    const taskFile = resolveTaskFile(taskType, command, node, projectArchive)
    if (taskFile) {
      files.push(taskFile)
    } else {
      for (let key of Object.keys(node)) {
        enumerateTaskFiles(node[key], files, projectArchive)
      }
    }
  }
}

function workflowFiles (workflow:Workflow, projectArchive:ProjectArchive):Array<TaskFile> {
  const files = []
  enumerateTaskFiles(workflow.config, files, projectArchive)
  return files
}

function fileString (file:string, projectArchive:?ProjectArchive) {
  if (!projectArchive) {
    return ''
  }
  const buffer = projectArchive.getFileContents(file)
  if (!buffer) {
    return ''
  }
  return buffer.toString()
}

const FileView = ({ file, fileType, contents }:{file: string, fileType: string, contents: string}) =>
  <div>
    <h4>{file}</h4>
    <div className='card bg-light my-2'>
      <div className='card-body p-2'>
        <pre className='mb-0'>
          <code>
            <Measure>
              { ({ width }) =>
                <CodeViewer
                  className='definition'
                  language={fileType}
                  value={contents}
                  style={{ width }}
                />
              }
            </Measure>
          </code>
        </pre>
      </div>
    </div>
  </div>

const WorkflowFilesView = ({ workflow, projectArchive }:{workflow: Workflow, projectArchive: ?ProjectArchive}) =>
  projectArchive ? <div>{
    workflowFiles(workflow, projectArchive).map(file =>
      <FileView key={file.name} file={file.name} fileType={file.fileType}
        contents={fileString(file.name, projectArchive)} />)
  }</div> : null

class AttemptView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    attemptId: string;
  };

  state = {
    attempt: null,
    done: false
  };

  componentDidMount () {
    this.fetch()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetch()
  }

  fetch () {
    model().fetchAttempt(this.props.attemptId).then(attempt => {
      if (!this.ignoreLastFetch) {
        this.setState({ attempt: attempt, done: attempt.done })
      }
    })
  }

  killAttempt () {
    model()
      .killAttempt(this.props.attemptId)
      .then(() => this.forceUpdate())
  }

  render () {
    const attempt = this.state.attempt
    const canKill = attemptCanBeKilled(attempt)

    if (!attempt) {
      return null
    }

    return (
      <div>
        <h2 className='d-inline-flex'>Attempt</h2>
        {canKill &&
          <button
            className='btn btn-sm btn-danger float-right'
            onClick={this.killAttempt.bind(this)}
          >
            KILL
          </button>
        }
        <table className='table table-sm'>
          <tbody>
            <tr>
              <td>ID</td>
              <td>{attempt.id}</td>
            </tr>
            <tr>
              <td>Project</td>
              <td><Link to={`/projects/${attempt.project.id}`}>{attempt.project.name}</Link></td>
            </tr>
            <tr>
              <td>Workflow</td>
              <td><Link to={`/workflows/${attempt.workflow.id}`}>{attempt.workflow.name}</Link></td>
            </tr>
            <tr>
              <td>Session ID</td>
              <td><Link to={`/sessions/${attempt.sessionId}`}>{attempt.sessionId}</Link></td>
            </tr>
            <tr>
              <td>Session UUID</td>
              <td><SessionTime t={attempt.sessionUuid} /></td>
            </tr>
            <tr>
              <td>Session Time</td>
              <td><SessionTime t={attempt.sessionTime} /></td>
            </tr>
            <tr>
              <td>Created</td>
              <td><FullTimestamp showAgo={Boolean(true)} t={attempt.createdAt} /></td>
            </tr>
            <tr>
              <td>Status</td>
              <td><AttemptStatusView attempt={attempt} /></td>
            </tr>
            <tr>
              <td>Params</td>
              <td><ParamsView params={attempt.params} /></td>
            </tr>
          </tbody>
        </table>
        <ReactInterval timeout={refreshIntervalMillis} enabled={!this.state.done} callback={() => this.fetch()} />
      </div>
    )
  }
}

class SessionView extends React.Component {
  props:{
    session: Session;
  };

  retryFailed () {
    const { session } = this.props
    const { lastAttempt } = session
    if (lastAttempt) {
      model()
        .resumeSessionWithLatestRevision(session, uuidv4(), lastAttempt.id)
        .then(() => this.forceUpdate())
    }
  }

  retryAll () {
    const { session } = this.props
    model()
      .retrySessionWithLatestRevision(session, uuidv4())
      .then(() => this.forceUpdate())
  }

  render () {
    const { session } = this.props
    const { lastAttempt, project, workflow } = session
    const canRetryAll = attemptCanRetryAll(lastAttempt)
    const canResume = attemptCanResume(lastAttempt)
    return (
      <div className='session'>
        <h2 className='d-inline-flex'>
          Session
        </h2>
        {canRetryAll &&
        <button
          className='btn btn-sm btn-primary float-right'
          onClick={this.retryAll.bind(this)}
        >
              RETRY ALL
        </button>
        }
        {canResume &&
        <button
          className='btn btn-sm btn-success mr-2 float-right'
          onClick={this.retryFailed.bind(this)}
        >
              RETRY FAILED
        </button>
        }
        <table className='table table-sm'>
          <tbody>
            <tr>
              <td>ID</td>
              <td>{session.id}</td>
            </tr>
            <tr>
              <td>Project</td>
              <td><Link to={`/projects/${project.id}`}>{project.name}</Link></td>
            </tr>
            <tr>
              <td>Workflow</td>
              <td><MaybeWorkflowLink workflow={workflow} /></td>
            </tr>
            <tr>
              <td>Revision</td>
              <td><SessionRevisionView session={session} /></td>
            </tr>
            <tr>
              <td>Session UUID</td>
              <td>{session.sessionUuid}</td>
            </tr>
            <tr>
              <td>Session Time</td>
              <td><SessionTime t={session.sessionTime} /></td>
            </tr>
            <tr>
              <td>Status</td>
              <td><SessionStatusView session={session} /></td>
            </tr>
            <tr>
              <td>Last Attempt</td>
              <td><FullTimestamp showAgo={Boolean(true)} t={lastAttempt && lastAttempt.createdAt} /></td>
            </tr>
            <tr>
              <td>Last Attempt Duration:</td>
              <td><DurationView start={lastAttempt && lastAttempt.createdAt} end={lastAttempt && lastAttempt.finishedAt} /></td>
            </tr>
            <tr>
              <td>Last Attempt Params:</td>
              <td><ParamsView params={lastAttempt && lastAttempt.params} /></td>
            </tr>
          </tbody>
        </table>
      </div>
    )
  }
}

const SessionTime = ({ t }:{t:?string}) =>
  t ? <span>{t}</span> : null

class Timestamp extends React.Component {
  props:{
    t: ?string
  }

  render () {
    const { t } = this.props
    if (!t) {
      return <span />
    }
    const m = moment(t)
    return (
      <span>
        <span>{m.fromNow()}</span>
        <ReactInterval timeout={1000} enabled={Boolean(true)} callback={() => this.forceUpdate()} />
      </span>
    )
  }
}

class FullTimestamp extends React.Component {
  props:{
    t: ?string,
    showAgo: boolean
  }

  timestamp: any;

  componentDidMount () {
    jQuery(this.timestamp).tooltip({ html: true })
  }

  tooltipText (t, m) {
    return `${t}<br/>${m.fromNow()}`
  }

  updateTime (t, m) {
    jQuery(this.timestamp)
      .attr('data-original-title', this.tooltipText(t, m))
      .show()
    this.forceUpdate()
  }

  render () {
    const { t, showAgo } = this.props
    if (!t) {
      return <span />
    }
    const m = moment(t)
    const duration = showAgo ? <span className='text-muted'> ({m.fromNow()})</span> : <span />
    return (
      <span>
        <span ref={(el) => { this.timestamp = el }} data-toggle='tooltip' data-placement='bottom' title={this.tooltipText(t, m)}>{m.format('YYYY-MM-DD HH:mm:ss')}{duration}</span>
        <ReactInterval timeout={1000} enabled={Boolean(true)} callback={() => this.updateTime(t, m)} />
      </span>
    )
  }
}

const Alerts = ({ alertType, message }:{alertType:string, message:string}) => {
  switch (alertType) {
    case 'success':
      return <div className='alert alert-success' role='alert'>{message}</div>
    case 'danger':
      return <div className='alert alert-danger' role='alert'>{message}</div>
    default:
      return <div role='alert'>{message}</div>
  }
}

const DurationView = ({ start, end }:{start:?string, end:?string}) => {
  if (!start || !end) {
    return <span />
  }
  const duration = new Duration(new Date(start), new Date(end)).toString(1, 1) // format: 10y 2m 6d 3h 23m 8s
  return <span>{duration}</span>
}

const ParamsView = ({ params }:{params: Object}) =>
  _.isEmpty(params)
    ? null
    : <CodeViewer className='params-view' language='yaml' value={yaml.safeDump(params, { sortKeys: true })} />

const TaskState = ({ state, cancelRequested }:{state: string, cancelRequested: boolean}) => {
  if (cancelRequested && ['ready', 'retry_waiting', 'group_retry_waiting', 'planned'].indexOf(state) >= 0) {
    // These state won't progress once cancelRequested is set. Planned tasks won't generate tasks.
    return <span><FontAwesomeIcon icon={faExclamationCircle} className='text-warning' /> Canceling</span>
  }

  switch (state) {
    // Pending
    case 'blocked':
      if (cancelRequested) {
        // Blocked tasks won't start once cancelRequested is set
        return <span><FontAwesomeIcon icon={faExclamationCircle} className='text-warning' /> Canceled</span>
      } else {
        return <span><FontAwesomeIcon icon={faSyncAlt} className='text-info' /> Blocked</span>
      }
    case 'ready':
      return <span><FontAwesomeIcon icon={faSyncAlt} className='text-info' /> Ready</span>
    case 'retry_waiting':
      return <span><FontAwesomeIcon icon={faSyncAlt} className='text-info' /> Retry Waiting</span>
    case 'group_retry_waiting':
      return <span><FontAwesomeIcon icon={faSyncAlt} className='text-info' /> Group Retry Waiting</span>
    case 'planned':
      return <span><FontAwesomeIcon icon={faSyncAlt} className='text-info' /> Planned</span>

    // Running
    case 'running':
      return <span><FontAwesomeIcon icon={faPlayCircle} className='text-info' /> Running</span>

    // Error
    case 'group_error':
      return <span><FontAwesomeIcon icon={faExclamationCircle} className='text-danger' /> Group Error</span>
    case 'error':
      return <span><FontAwesomeIcon icon={faExclamationCircle} className='text-danger' /> Error</span>

    // Warning
    case 'canceled':
      return <span><FontAwesomeIcon icon={faExclamationCircle} className='text-warning' /> Canceled</span>

    // Success
    case 'success':
      return <span><FontAwesomeIcon icon={faCheckCircle} className='text-success' /> Success</span>

    default:
      return <span>{_.capitalize(state)}</span>
  }
}

const JobLink = ({ storeParams, stateParams }:{storeParams: Object, stateParams: Object}) => {
  const paramsJobId = storeParams.td && storeParams.td.last_job_id
  const stateJobId = stateParams.job && stateParams.job.jobId
  const jobId = paramsJobId || stateJobId
  const link = DIGDAG_CONFIG.td.jobUrl(jobId)
  if (!jobId) {
    return null
  }
  return <a href={link} target='_blank'>{jobId}</a>
}

function sortTasksForTreeView (tasks: Array<Task>): Array<Task> {
  function collectChildrenRecursively (result: Array<Task>, taskGroups: Map<string, Array<Task>>, parentTask: Task) {
    const group: ?Array<Task> = taskGroups.get(parentTask.id)
    if (group != null) {
      taskGroups.delete(parentTask.id)
      group.forEach(t => {
        result.push(t)
        collectChildrenRecursively(result, taskGroups, t)
      })
    }
  }

  // First, divide tasks into rootTasks and taskGroups.
  const rootTasks: Array<Task> = []
  const taskGroups: Map<string, Array<Task>> = new Map() // {parentId => Array<Task>}
  const taskOrders: Map<string, Number> = new Map()
  tasks.forEach(t => {
    const parentId: ?string = t.parentId
    if (taskOrders.get(t.fullName) == null) {
      taskOrders.set(t.fullName, 1)
    } else {
      taskOrders.set(t.fullName, taskOrders.get(t.fullName) + 1)
    }
    t.order = taskOrders.get(t.fullName)

    if (parentId != null) {
      const group: ?Array<Task> = taskGroups.get(parentId)
      if (group != null) {
        group.push(t)
      } else {
        taskGroups.set(parentId, [t])
      }
    } else {
      rootTasks.push(t)
    }
  })

  // For each root task, push it to the result, and push its children to the result.
  const result: Array<Task> = []
  rootTasks.forEach(t => {
    result.push(t)
    // collect children recursively
    collectChildrenRecursively(result, taskGroups, t)
  })

  // This is actually unnecessary but in case something is wrong, concatenate all
  // remaining task groups at the end.
  Array.from(taskGroups.values()).forEach(remainingGroup => {
    Array.prototype.push.apply(result, remainingGroup)
  })

  return result
}

class TaskListView extends React.Component {
  props:{
    tasks: Map<string, Task>
  }

  render () {
    return (
      <div className='table-responsive'>
        <table className='table table-striped table-hover table-sm'>
          <thead>
            <tr>
              <th>ID</th>
              <th>Job</th>
              <th>Name</th>
              <th>Parent ID</th>
              <th>Started</th>
              <th>Updated</th>
              <th>State</th>
              <th>Retry</th>
              <th>State Params</th>
              <th>Store Params</th>
            </tr>
          </thead>
          <tbody>
            {
              sortTasksForTreeView(Array.from(this.props.tasks.values())).map(task =>
                <tr key={task.id}>
                  <td>
                    {task.parentId != null ? <a href={'#logs-' + task.fullName + task.order}>{task.id}</a> : task.id}
                  </td>
                  <td><JobLink storeParams={task.storeParams} stateParams={task.stateParams} /></td>
                  <td>{task.fullName}</td>
                  <td>{task.parentId}</td>
                  <td><FullTimestamp showAgo={false} t={task.startedAt} /></td>
                  <td><FullTimestamp showAgo={false} t={task.updatedAt} /></td>
                  <td><TaskState state={task.state} cancelRequested={task.cancelRequested} /></td>
                  <td><Timestamp t={task.retryAt} /></td>
                  <td><ParamsView params={task.stateParams} /></td>
                  <td><ParamsView params={task.storeParams} /></td>
                </tr>
              )
            }
          </tbody>
        </table>
      </div>
    )
  }
}

function taskDone (task: Task): boolean {
  switch (task.state) {
    case 'success':
    case 'group_error':
    case 'error':
    case 'canceled':
      return true

    default:
      return false
  }
}

function isSyntheticTask (task: Task): boolean {
  // XXX: For task generating operators like loop> and for_each> etc, digdag synthesizes a grouping task to
  //      hold the generated child tasks. The name of this task is hardcoded to end with ^sub.
  return task.fullName.endsWith('^sub')
}

class TaskTimelineRow extends React.Component {
  props:{
    task: Task;
    tasks: Map<string, Task>;
    startTime: ?Object;
    endTime: ?Object;
  };

  progressBar: any;

  componentDidMount () {
    jQuery(this.progressBar).tooltip({ html: true })
  }

  progressBarClasses () {
    if (this.props.task.cancelRequested && ['ready', 'retry_waiting', 'group_retry_waiting', 'planned'].indexOf(this.props.task.state) >= 0) {
      return 'progress-bar-warning'
    }

    switch (this.props.task.state) {
      // Pending
      case 'blocked':
        return ''

      // Running
      case 'ready':
      case 'planned':
      case 'retry_waiting':
      case 'group_retry_waiting':
      case 'running':
        return 'progress-bar-info progress-bar-striped active'

      // Error
      case 'group_error':
      case 'error':
        return 'bg-danger'

      // Warning
      case 'canceled':
        return 'bg-warning'

      // Success
      case 'success':
        return 'bg-success'

      default:
        return ''
    }
  }

  taskLevel () {
    let task = this.props.task
    const tasks = this.props.tasks
    let level = 0
    while (task != null && task.parentId != null) {
      const parentId = task.parentId
      if (!isSyntheticTask(task)) {
        level++
      }
      task = tasks.get(parentId)
    }
    return level
  }

  render () {
    const { startTime, endTime, task, tasks } = this.props
    const parentTask = tasks.get(task.parentId || '')
    const namePrefix = parentTask != null ? parentTask.fullName : ''
    const taskName = task.fullName.substring(namePrefix.length)
    let style = {}
    let duration = ''
    let tooltip = ''
    if (startTime == null || endTime == null || task.startedAt == null || task.updatedAt == null) {
      style = { width: 0 }
    } else {
      const totalMillis = endTime.valueOf() - startTime.valueOf()
      const taskStartedAt = moment(task.startedAt)
      const taskUpdatedAt = moment(task.updatedAt)
      const taskEndTime = taskDone(task) ? taskUpdatedAt : moment()
      const taskDuration = moment.duration(taskEndTime.diff(taskStartedAt))
      const taskRelStartMillis = 1.0 * (taskStartedAt.valueOf() - startTime.valueOf())
      const taskRelEndMillis = 1.0 * (taskEndTime.valueOf() - startTime.valueOf())
      const taskStartPct = 100.0 * (taskRelStartMillis / totalMillis)
      const taskEndPct = 100.0 * (taskRelEndMillis / totalMillis)
      const marginLeft = taskStartPct
      const marginRight = 100.0 - taskEndPct
      const width = taskEndPct - taskStartPct
      style = {
        marginLeft: `${marginLeft}%`,
        width: `${width}%`,
        marginRight: `${marginRight}%`,
        // http://stackoverflow.com/a/13293044
        // ¯\_(ツ)_/¯
        transform: 'translateZ(0)'
      }
      duration = taskDuration.format('d[d] h[h] mm[m] ss[s]')
      tooltip = `${task.startedAt || ''} -<br/>${task.updatedAt || ''}`
    }
    return (
      <tr>
        <td style={{ whiteSpace: 'nowrap', paddingLeft: `${this.taskLevel()}em` }}>{taskName}</td>
        <td className='align-middle' style={{ width: '100%' }}>
          <div className='progress mb-0　' style={{ height: '1.4rem' }}>
            <div ref={(em) => { this.progressBar = em }} data-toggle='tooltip' data-placement='bottom' title={tooltip}
              className={`progress-bar ${this.progressBarClasses()}`} role='progressbar' style={style}>{duration}</div>
          </div>
        </td>
      </tr>
    )
  }
}

const TaskTimelineView = ({ tasks, startTime, endTime }:{
  tasks: Map<string, Task>;
  startTime: ?Object;
  endTime: ?Object;
}) =>
  <div className='table-responsive'>
    <table className='table table-sm'>
      <thead>
        <tr>
          <th>Task</th>
          <th>Execution</th>
        </tr>
      </thead>
      <tbody>
        { sortTasksForTreeView(Array.from(tasks.values()))
          .filter(task => task.parentId != null)
          .filter(task => !isSyntheticTask(task))
          .map(task =>
            <TaskTimelineRow key={task.id} task={task} tasks={tasks} startTime={startTime} endTime={endTime} />)
        }
      </tbody>
    </table>
  </div>

class AttemptTasksView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    attemptId: string;
  };

  state = {
    tasks: new Map(),
    done: false
  };

  componentDidMount () {
    this.fetch()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetch()
  }

  fetch () {
    model().fetchAttemptTasks(this.props.attemptId).then(taskMap => {
      if (!this.ignoreLastFetch) {
        const tasks = Array.from(taskMap.values())
        const done = tasks.every(task => taskDone(task))
        this.setState({ tasks: taskMap, done })
      }
    })
  }

  render () {
    const { done } = this.state
    return (
      <div>
        <h2>Tasks</h2>
        <TaskListView tasks={this.state.tasks} />
        <ReactInterval timeout={refreshIntervalMillis} enabled={!done} callback={() => this.fetch()} />
      </div>
    )
  }
}

class AttemptTimelineView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    attemptId: string;
  };

  state = {
    tasks: new Map(),
    done: false,
    firstStartedAt: null,
    lastUpdatedAt: null,
    endTime: null
  };

  componentDidMount () {
    this.fetch()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetch()
  }

  firstStartedAt (tasks: Array<Task>): ?Object {
    return tasks
      .filter(task => task.startedAt !== null)
      .map(task => moment(task.startedAt))
      .reduce((first, startedAt) => first === null || startedAt.isBefore(first) ? startedAt : first, null)
  }

  lastUpdatedAt (tasks: Array<Task>): ?Object {
    return tasks
      .filter(task => task.updatedAt !== null)
      .map(task => moment(task.updatedAt))
      .reduce((last, updatedAt) => last === null || updatedAt.isAfter(last) ? updatedAt : last, null)
  }

  fetch () {
    model().fetchAttemptTasks(this.props.attemptId).then(taskMap => {
      if (!this.ignoreLastFetch) {
        const tasks = Array.from(taskMap.values())
        const done = tasks.every(task => taskDone(task))
        const lastUpdatedAt = this.lastUpdatedAt(tasks)
        const firstStartedAt = this.firstStartedAt(tasks)
        const endTime = this.endTime(done, lastUpdatedAt)
        this.setState({
          tasks: taskMap,
          done,
          firstStartedAt,
          lastUpdatedAt,
          endTime
        })
      }
    })
  }

  updateTime () {
    const { done, lastUpdatedAt } = this.state
    this.setState({ endTime: this.endTime(done, lastUpdatedAt) })
  }

  endTime (done: boolean, lastUpdatedAt: ?Object) {
    if (done) {
      return lastUpdatedAt
    } else {
      return moment().add(1, 'minute').startOf('minute')
    }
  }

  render () {
    const { done } = this.state
    return (
      <div>
        <h2>Timeline</h2>
        <TaskTimelineView tasks={this.state.tasks} startTime={this.state.firstStartedAt} endTime={this.state.endTime} />
        <ReactInterval timeout={refreshIntervalMillis} enabled={!done} callback={() => this.fetch()} />
        <ReactInterval timeout={200} enabled={!done} callback={() => this.updateTime()} />
      </div>
    )
  }
}

class LogFileView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    attemptId: string;
    file: LogFileHandle;
    order: Number;
  }

  state = {
    data: null
  };

  componentDidMount () {
    this.fetchFile()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetchFile()
  }

  shouldComponentUpdate (nextProps, nextState) {
    return this.state.data == null
  }

  fetchFile () {
    model().fetchLogFile(this.props.attemptId, this.props.file).then(data => {
      if (!this.ignoreLastFetch) {
        this.setState({ data })
      }
    }, error => console.log(error))
  }

  render () {
    if (this.state.data) {
      return (
        <div>
          <h3 id={'logs-' + this.props.file.taskName + this.props.order.toString()}
            className='log-view'><small>{this.props.file.taskName}</small></h3>

          <div className='card bg-light my-2'>
            <div className='card-body p-2'>
              <pre className='m-0 small'>
                <code>
                  {pako.inflate(this.state.data, { to: 'string' })}
                </code>
              </pre>
            </div>
          </div>
        </div>
      )
    } else {
      return null
    }
  }
}

class AttemptLogsView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    attemptId: string;
  };

  state = {
    files: [],
    done: false
  };

  componentDidMount () {
    this.fetch()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetch()
  }

  logFiles () {
    const { files } = this.state
    if (!files.length) {
      return <pre />
    }
    const taskOrders: Map<string, Number> = new Map()
    return this.state.files.map(file => {
      if (taskOrders.get(file.taskName) == null) {
        taskOrders.set(file.taskName, 1)
      } else {
        taskOrders.set(file.taskName, taskOrders.get(file.taskName) + 1)
      }

      return <LogFileView key={file.fileName} file={file} attemptId={this.props.attemptId} order={taskOrders.get(file.taskName)} />
    })
  }

  fetch () {
    const { attemptId } = this.props
    model().fetchAttempt(attemptId).then(attempt => {
      if (!this.ignoreLastFetch) {
        this.setState({ done: attempt.done })
      }
    })
    model().fetchAttemptLogFileHandles(attemptId).then(({ files }) => {
      if (!this.ignoreLastFetch) {
        const sortedFiles = _.sortBy(files, 'fileTime')
        this.setState({ files: sortedFiles })
      }
    })
  }

  render () {
    const { done } = this.state
    return (
      <div>
        <h2>Logs</h2>
        {this.logFiles()}
        <ReactInterval timeout={refreshIntervalMillis} enabled={!done} callback={() => this.fetch()} />
      </div>
    )
  }
}

class VersionView extends React.Component {
  state = {
    version: ''
  };

  componentDidMount () {
    model().fetchVersion().then(version => {
      this.setState(version)
    })
  }

  render () {
    return (
      <span>{this.state.version}</span>
    )
  }
}

class Navbar extends React.Component {
  static propTypes = {
    match: PropTypes.object.isRequired,
    location: PropTypes.object.isRequired,
    history: PropTypes.object.isRequired
  }

  logout (e) {
    e.preventDefault()
    window.localStorage.removeItem('digdag.credentials')
    window.location = DIGDAG_CONFIG.logoutUrl
  }

  brand () {
    const navbar = DIGDAG_CONFIG.navbar
    return navbar ? navbar.brand : 'Digdag'
  }

  logo () {
    const navbar = DIGDAG_CONFIG.navbar
    return navbar && navbar.logo
      ? <a className='navbar-brand' href='/' style={{ marginTop: '-7px' }}><img src={navbar.logo} width='36' height='36' /></a>
      : null
  }

  className () {
    const { navbar } = DIGDAG_CONFIG
    return navbar && navbar.className ? navbar.className : 'navbar-inverse'
  }

  style () {
    const navbar = DIGDAG_CONFIG.navbar
    return navbar && navbar.style ? navbar.style : {}
  }

  isActiveClass (path) {
    const { location } = this.props
    const match = matchPath(location.pathname, {
      path: path,
      exact: true
    })
    return match ? 'active' : ''
  }

  render () {
    return (
      <nav className={`navbar ${this.className()} navbar-dark navbar-expand-lg`} style={this.style()}>
        {this.logo()}
        <a className='navbar-brand' href='/'>{this.brand()}</a>
        <button type='button' className='navbar-toggler' data-toggle='collapse' data-target='#navbar'
          aria-expanded='false' aria-controls='navbar' aria-label='Toggle navigation'>
          <span className='sr-only'>Toggle navigation</span>
          <span className='navbar-toggler-icon' />
        </button>
        <div id='navbar' className='collapse navbar-collapse'>
          <ul className='navbar-nav mr-auto'>
            <li className={`nav-item ${this.isActiveClass('/')}`}><Link className='nav-link' to='/'>Workflows</Link></li>
            <li className={`nav-item ${this.isActiveClass('/projects')}`}><Link className='nav-link' to='/projects'>Projects</Link></li>
          </ul>
          <li className='navbar-text navbar-right'><VersionView /></li>
          <ul className='nav navbar-nav navbar-right'>
            <li><a className='nav-link' href='/' onClick={this.logout}><FontAwesomeIcon icon={faSignOutAlt} /> Logout</a></li>
          </ul>
        </div>
      </nav>
    )
  }
}

const ProjectsPage = (props:{}) =>
  <div className='container-fluid'>
    <ProjectsView />
    <SessionsView />
  </div>

const WorkflowsPage = () =>
  <div className='container-fluid'>
    <div className='float-right'>
      <Link
        className='btn btn-sm btn-outline-secondary'
        to={`/projects/new`}
      >
        New project
      </Link>
    </div>
    <WorkflowsView />
    <SessionsView />
  </div>

const ProjectPage = (props:{params: {projectId: string}}) =>
  <div className='container-fluid'>
    <ProjectView projectId={props.match.params.projectId} />
  </div>

class WorkflowPage extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    params: {
      projectId: string;
      workflowName: string;
    }
  };

  state:{
    workflow: ?Workflow;
  };

  constructor (props) {
    super(props)
    this.state = {
      workflow: null
    }
  }

  componentDidMount () {
    this.fetch()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetch()
  }

  fetch () {
    model().fetchProjectWorkflow(this.props.match.params.projectId, this.props.match.params.workflowName).then(workflow => {
      if (!this.ignoreLastFetch) {
        this.setState({ workflow })
      }
    })
  }

  workflow () {
    return this.state.workflow ? <WorkflowView workflow={this.state.workflow} /> : null
  }

  render () {
    return (
      <div className='container-fluid'>
        {this.workflow()}
        <ReactInterval timeout={refreshIntervalMillis} enabled={Boolean(true)} callback={() => this.fetch()} />
      </div>
    )
  }
}

class WorkflowRevisionPage extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    params: {
      workflowId: string;
    };
  };

  state:{
    workflow: ?Workflow;
  };

  constructor (props) {
    super(props)
    this.state = {
      workflow: null
    }
  }

  componentDidMount () {
    this.fetchWorkflow()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetchWorkflow()
  }

  fetchWorkflow () {
    model().fetchWorkflow(this.props.match.params.workflowId).then(workflow => {
      if (!this.ignoreLastFetch) {
        this.setState({ workflow })
      }
    })
  }

  workflow () {
    return this.state.workflow ? <WorkflowView workflow={this.state.workflow} /> : null
  }

  render () {
    return (
      <div className='container-fluid'>
        {this.workflow()}
      </div>
    )
  }
}

class FileEditor extends React.Component {
  editor: ?CodeEditor

  props:{
    file: ?TarEntry;
    projectArchive: ?ProjectArchive;
    onDelete: Function;
  }

  state:{
    name: string;
  }

  constructor (props) {
    super(props)
    this.state = {
      name: props.file ? props.file.name : 'new.dig'
    }
  }

  getName () {
    return this.state.name
  }

  getFileContents () {
    var text
    if (this.editor) {
      text = this.editor.getValue()
    } else {
      text = this.props.file ? fileString(this.props.file.name, this.props.projectArchive) : ''
    }

    const buffer = new Buffer(text)
    const ab = new ArrayBuffer(buffer.length)
    const view = new Uint8Array(ab)
    view.set(buffer)
    return ab
  }

  render () {
    const file = this.props.file
    return (
      <div>
        <div className='input-group mb-2'>
          <input type='text' className='form-control' value={this.state.name} onChange={this.handleNameChange.bind(this)} />
          <div className='input-group-append'>
            <button className='btn btn-sm btn-outline-danger float-right' onClick={this.props.onDelete}>Delete</button>
          </div>
        </div>
        <pre>
          <Measure>
            { ({ width }) =>
              <CodeEditor
                className='editor'
                language='yaml' // TODO how to let ace guess language?
                value={file ? fileString(file.name, this.props.projectArchive) : ''}
                style={{ width }}
                ref={(value) => { this.editor = value }}
              />
            }
          </Measure>
        </pre>
      </div>
    )
  }

  handleNameChange (event) {
    this.setState({ name: event.target.value })
  }
}

class ProjectArchiveEditor extends React.Component {
  _editors: Object;

  props:{
    projectArchive: ?ProjectArchive;
  }

  state:{
    entries: [];
  }

  constructor (props) {
    super(props)
    this.state = {
      entries: []
    }
    this._editors = {}
  }

  handleDelete (key) {
    _.remove(this.state.entries, (file) => file.key === key)
    this.setState({ entries: this.state.entries })
  }

  handleAddFile () {
    const key = uuidv4()
    const entries = [
      {
        newFile: true,
        key,
        file: null,
        projectArchive: null
      },
      ...this.state.entries
    ]
    this.setState({ entries })
  }

  setInitialEntries () {
    const projectFiles = this.props.projectArchive ? this.props.projectArchive.files : []
    const entries = projectFiles.map(file => ({
      newFile: false,
      key: file.name,
      file: file,
      projectArchive: this.props.projectArchive
    }))
    this.setState({ entries })
  }

  componentDidMount () {
    this.setInitialEntries()
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.setInitialEntries()
  }

  render () {
    const editors = this.state.entries.map(entry =>
      <FileEditor
        newFile={entry.newFile}
        key={entry.key}
        ref={(value) => { this._editors[entry.key] = value }}
        file={entry.file}
        projectArchive={entry.projectArchive}
        onDelete={this.handleDelete.bind(this, entry.key)}
      />
    )
    return (
      <div>
        <div className='btn-group mb-2 pb-2'>
          <button className='btn btn-light btn-sm' onClick={this.handleAddFile.bind(this)}>Add file</button>
        </div>
        {editors}
      </div>
    )
  }

  getFiles (): Array<TarEntry> {
    return this.state.entries.map(entry => {
      const editor = this._editors[entry.key]
      return ({ name: editor.getName(), buffer: editor.getFileContents() })
    })
  }
}

class ProjectEditor extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    projectId: ?string;
  };

  state = {
    projectId: null,
    project: null,
    projectName: 'new-project',
    revisionName: uuidv4(),
    projectArchive: null,
    saveMessage: '',
    alertType: ''
  };

  componentDidMount () {
    this.fetch()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetch()
  }

  fetch () {
    const projectId = this.props.projectId
    if (projectId) {
      model().fetchProject(projectId).then(project => {
        if (!this.ignoreLastFetch) {
          this.setState({ project: project, projectName: project.name })
        }
        return project
      }).then(project => {
        if (!this.ignoreLastFetch) {
          model().fetchProjectArchiveWithRevision(project.id, project.revision).then(projectArchive => {
            if (!this.ignoreLastFetch) {
              this.setState({ projectArchive })
            }
          })
        }
      })
    }
  }

  _editor: any

  render () {
    const project = this.state.project
    var title
    var header
    if (project) {
      title = 'Edit Workflows'
      header = (
        <table className='table table-sm'>
          <tbody>
            <tr>
              <td>ID</td>
              <td>{project.id}</td>
            </tr>
            <tr>
              <td>Name</td>
              <td>{project.name}</td>
            </tr>
            <tr>
              <td>Current revision</td>
              <td>{project.revision}</td>
            </tr>
            <tr>
              <td>Created</td>
              <td><FullTimestamp showAgo={Boolean(true)} t={project.createdAt} /></td>
            </tr>
            <tr>
              <td>Updated</td>
              <td><FullTimestamp showAgo={Boolean(true)} t={project.updatedAt} /></td>
            </tr>
          </tbody>
        </table>
      )
    } else {
      title = 'New Project'
      header = (
        <table className='table table-sm'>
          <tbody>
            <tr>
              <td>Name</td>
              <td><input type='text' className='form-control form-control-sm' value={this.state.projectName} onChange={this.handleNameChange.bind(this)} /></td>
            </tr>
            <tr>
              <td>Revision</td>
              <td><input type='text' className='form-control form-control-sm' value={this.state.revisionName} onChange={this.handleRevisionChange.bind(this)} /></td>
            </tr>
          </tbody>
        </table>
      )
    }
    return (
      <div>
        <h2>{title}</h2>
        {header}
        <Alerts alertType={this.state.alertType} message={this.state.saveMessage} />
        <button className='btn mb-2 btn-sm btn-info' onClick={this.save.bind(this)}>Save</button>
        <ProjectArchiveEditor projectArchive={this.state.projectArchive} ref={(value) => { this._editor = value }} />
      </div>
    )
  }

  handleNameChange (event) {
    this.setState({ projectName: event.target.value })
  }

  handleRevisionChange (event) {
    this.setState({ revisionName: event.target.value })
  }

  save () {
    const files = this._editor.getFiles()
    const archive = new Tar()
    var out = ''
    for (let file of files) {
      out = archive.append(file.name, new Uint8Array(file.buffer))
    }
    const targz = pako.gzip(out)
    model().putProject(this.state.projectName, this.state.revisionName, targz).then(project => {
      this.setState({
        projectId: project.id,
        project: project,
        revisionName: uuidv4(), // generate new revision name
        alertType: 'success',
        saveMessage: `Revision ${this.state.revisionName} is saved.`
      })
    }).catch((error) => {
      console.log(`Saving project failed`, error)
      this.setState({
        alertType: 'danger',
        saveMessage: `Failed to store: ${error.message}`
      })
    })
  }
}

const NewProjectPage = (props:{}) =>
  <div className='container-fluid'>
    <ProjectEditor projectId={null} />
  </div>

const EditProjectPage = (props:{params: {projectId: string}}) =>
  <div className='container-fluid'>
    <ProjectEditor projectId={props.match.params.projectId} />
  </div>

const AttemptPage = ({ match }:{params: {attemptId: string}}) =>
  <div className='container-fluid'>
    <AttemptView attemptId={match.params.attemptId} />
    <AttemptTimelineView attemptId={match.params.attemptId} />
    <AttemptTasksView attemptId={match.params.attemptId} />
    <AttemptLogsView attemptId={match.params.attemptId} />
  </div>

class SessionPage extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    params: {
      sessionId: string;
    }
  };

  state:{
    session: ?Session;
    attempts: Array<Attempt>;
  };

  constructor (props) {
    super(props)
    this.state = {
      session: null,
      tasks: [],
      attempts: []
    }
  }

  componentDidMount () {
    this.fetch()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetch()
  }

  fetch () {
    model().fetchSession(this.props.match.params.sessionId).then(session => {
      if (!this.ignoreLastFetch) {
        this.setState({ session })
      }
    })
    model().fetchSessionAttempts(this.props.match.params.sessionId).then(({ attempts }) => {
      if (!this.ignoreLastFetch) {
        this.setState({ attempts })
      }
    })
  }

  session () {
    return this.state.session
      ? <SessionView session={this.state.session} />
      : null
  }

  tasks () {
    return this.state.session && this.state.session.lastAttempt
      ? <AttemptTasksView attemptId={this.state.session.lastAttempt.id} />
      : null
  }

  timeline () {
    return this.state.session && this.state.session.lastAttempt
      ? <AttemptTimelineView attemptId={this.state.session.lastAttempt.id} />
      : null
  }

  logs () {
    return this.state.session && this.state.session.lastAttempt
      ? <AttemptLogsView attemptId={this.state.session.lastAttempt.id} />
      : <pre />
  }

  attempts () {
    return this.state.attempts
      ? <AttemptListView attempts={this.state.attempts} />
      : null
  }

  render () {
    return (
      <div className='container-fluid'>
        {this.session()}
        {this.timeline()}
        {this.tasks()}
        {this.logs()}
        {this.attempts()}
        <ReactInterval timeout={refreshIntervalMillis} enabled={Boolean(true)} callback={() => this.fetch()} />
      </div>
    )
  }
}

class LoginPage extends React.Component {
  props:{
    onSubmit: (credentials:Credentials) => void;
  };

  state:Credentials;

  constructor (props) {
    super(props)
    this.state = {}
    DIGDAG_CONFIG.auth.items.forEach(item => {
      this.state[item.key] = ''
    })
  }

  componentWillMount () {
    if (!DIGDAG_CONFIG.auth.items.length) {
      this.props.onSubmit({})
    }
  }

  onChange (key) {
    return (e) => {
      e.preventDefault()
      const state = {}
      state[key] = e.target.value
      this.setState(state)
    }
  }

  valid (credentials:Credentials, key:string, value:string) {
    return (key:string) => {
      credentials[key] = value
      if (DIGDAG_CONFIG.auth.items.length === Object.keys(credentials).length) {
        this.props.onSubmit(credentials)
      }
    }
  }

  invalid (values, key, value, message = '') {
    return (key) => {
      console.log(`${key} is invalid: message=${message})`)
    }
  }

  handleSubmit = (e) => {
    e.preventDefault()
    const credentials:Credentials = {}
    for (let item of DIGDAG_CONFIG.auth.items) {
      const key = item.key
      const scrub:Scrubber = item.scrub ? item.scrub : (args:{key: string, value: string}) => value
      const value:string = scrub({ key, value: this.state[key] })
      item.validate({
        key,
        value,
        valid: this.valid(credentials, key, value),
        invalid: this.invalid(credentials, key, key)
      })
    }
  };

  render () {
    const authItems = DIGDAG_CONFIG.auth.items.map(item => {
      return (
        <div className='form-group' key={item.key}>
          <label for={item.key}>{item.name}</label>
          <input
            type={item.type}
            className='form-control'
            onChange={this.onChange(item.key)}
            value={this.state[item.key]}
          />
        </div>
      )
    })

    return (
      <div className='container-fluid'>
        <h1>{DIGDAG_CONFIG.auth.title}</h1>
        <form onSubmit={this.handleSubmit}>
          {authItems}
          <button type='submit' className='btn btn-default'>Submit</button>
        </form>
      </div>
    )
  }
}

class WorkflowsView extends React.Component {
  state = {
    workflows: []
  };

  componentDidMount () {
    this.fetch()
  }

  fetch () {
    model().fetchWorkflows().then(({ workflows }) => {
      this.setState({ workflows })
    })
  }

  render () {
    return (
      <div className='workflows'>
        <h2>Workflows</h2>
        <WorkflowListView workflows={this.state.workflows} />
        <ReactInterval timeout={refreshIntervalMillis} enabled={Boolean(true)} callback={() => this.fetch()} />
      </div>
    )
  }
}

class NotFoundPage extends React.Component {
  componentDidMount () {
    this.props.history.replace('/')
  }

  render () {
    return null
  }
}

class ParserTest extends React.Component {
  definition () {
    return `
      timezone: UTC

      schedule:
        daily>: "07:00:00"

      _export:
        td:
          database: se379
          table: fbtest
        reload_window: 10
        start_from: "\${session_unixtime - (86400 * (reload_window - 1))}"

      +delete_records:
        td_partial_delete>: \${td.table}
        to: "\${session_time}"
        from: "\${start_from}"

      +import_from_facebook:
        td_load>: imports/facebook_ads_reporting.yml
        table: \${td.table}
    `
  }
  render () {
    return (
      <div className='container-fluid'>
        <CodeViewer className='definition' language='digdag' value={this.definition()} />
      </div>
    )
  }
}

class AppWrapper extends React.Component {
  render () {
    const NavbarWithRouter = withRouter(Navbar)
    return (
      <div className='container-fluid'>
        <NavbarWithRouter />
        <div className='app-wrapper'>
          {this.props.children}
        </div>
      </div>
    )
  }
}

export class CodeViewerTest extends React.Component {
  exampleSQL () {
    return `
      SELECT EmployeeID, FirstName, LastName, HireDate, City
      FROM Employees
      WHERE FirstName = 'Daniele'
    `
  }
  exampleYAML () {
    return `
      td_load>: imports/facebook_ads_reporting.yml
      td_run>: dan_test
      foo: bar
    `
  }
  render () {
    return (
      <div className='container-fluid'>
        <CodeViewer
          className='large-editor'
          language='sql'
          value={this.exampleSQL()} />
        <CodeViewer
          className='large-editor'
          language='digdag'
          value={this.exampleYAML()} />
      </div>
    )
  }
}

class ConsolePage extends React.Component {
  render () {
    return (
      <div className='container-fluid'>
        <Router>
          <CacheLoader>
            <AppWrapper>
              <Switch>
                <Route exact path='/' component={WorkflowsPage} />
                <Route exact path='/projects' component={ProjectsPage} />
                <Route exact path='/projects/new' component={NewProjectPage} />
                <Route exact path='/projects/:projectId' component={ProjectPage} />
                <Route exact path='/projects/:projectId/edit' component={EditProjectPage} />
                <Route exact path='/projects/:projectId/workflows/:workflowName' component={WorkflowPage} />
                <Route exact path='/workflows/:workflowId' component={WorkflowRevisionPage} />
                <Route exact path='/sessions/:sessionId' component={SessionPage} />
                <Route exact path='/attempts/:attemptId' component={AttemptPage} />
                {isDevelopmentEnv &&
                  <Switch>
                    <Route exact path='/parser-test' component={ParserTest} />
                    <Route exact path='/codeviewer' component={CodeViewerTest} />
                  </Switch>
                }
                <Route component={withRouter(NotFoundPage)} />
              </Switch>
            </AppWrapper>
          </CacheLoader>
        </Router>
      </div>
    )
  }
}

export default class Console extends React.Component {
  state:{
    authenticated: bool
  };

  constructor (props:any) {
    super(props)
    if (!DIGDAG_CONFIG.auth.items.length) {
      this.state = { authenticated: true }
      this.setup({})
    } else {
      const credentials = window.localStorage.getItem('digdag.credentials')
      if (credentials) {
        this.setup(JSON.parse(credentials))
        this.state = { authenticated: true }
      } else {
        this.state = { authenticated: false }
      }
    }
  }

  setup (credentials:Credentials) {
    setupModel({
      url: DIGDAG_CONFIG.url,
      td: DIGDAG_CONFIG.td,
      credentials: credentials,
      headers: DIGDAG_CONFIG.headers
    })
  }

  handleCredentialsSubmit:(credentials:Credentials) => void = (credentials:Credentials) => {
    window.localStorage.setItem('digdag.credentials', JSON.stringify(credentials))
    this.setup(credentials)
    this.setState({ authenticated: true })
  };

  render () {
    if (this.state.authenticated) {
      return <ConsolePage />
    } else {
      return <LoginPage onSubmit={this.handleCredentialsSubmit} />
    }
  }
}
