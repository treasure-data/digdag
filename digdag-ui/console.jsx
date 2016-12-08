// @flow
import './style.less'

import 'babel-polyfill'
import 'whatwg-fetch'

import _ from 'lodash'

import React from 'react'
import { Router, Link, Route, browserHistory, withRouter } from 'react-router'
import Measure from 'react-measure'
import moment from 'moment'
import pako from 'pako'
import path from 'path'
import yaml from 'js-yaml'
import Duration from 'duration'
import uuid from 'node-uuid'

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
  Task,
  Workflow
} from './model'

import {
  model,
  setup as setupModel
} from './model'
/* eslint-enable */

type Scrubber = (args:{key: string, value: string}) => string

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
    Ace.acequire('ace/ext/language_tools')
    Ace.acequire('ace/ext/linking')
  }

  componentDidMount () {
    const Ace = require('brace')
    require('./ace-digdag')
    this._editor = Ace.edit(this.editor)
    this._editor.setOptions({
      enableLinking: true,
      highlightActiveLine: false,
      readOnly: true,
      showLineNumbers: false,
      showGutter: false,
      showPrintMargin: false,
      tabSize: 2,
      useSoftTabs: true
    })
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
        <div className='loadingContainer'>
          <span className='glyphicon glyphicon-refresh spinning' />
          <span className='loadingText'>Loading ...</span>
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
        <td>{formatTimestamp(project.updatedAt)}</td>
        <td>{project.revision}</td>
      </tr>
    )
    return (
      <div className='table-responsive'>
        <table className='table table-striped table-hover table-condensed'>
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
        <table className='table table-striped table-hover table-condensed'>
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

function attemptStatus (attempt) {
  if (attempt.done) {
    if (attempt.success) {
      return <span><span className='glyphicon glyphicon-ok text-success' /> Success</span>
    } else {
      return <span><span className='glyphicon glyphicon-exclamation-sign text-danger' /> Failure</span>
    }
  } else {
    if (attempt.cancelRequested) {
      return <span><span className='glyphicon glyphicon-exclamation-sign text-warning' /> Canceling</span>
    } else {
      return <span><span className='glyphicon glyphicon-refresh text-info' /> Pending</span>
    }
  }
}

function attemptCanRetry (attempt) {
  if (!attempt) {
    return false
  }
  if (
    (attempt.done && !attempt.success) ||
    attempt.cancelRequested
  ) {
    return true
  }
  return false
}

const SessionStatusView = (props:{session: Session}) => {
  const attempt = props.session.lastAttempt
  return attempt
    ? <Link to={`/attempts/${attempt.id}`}>{attemptStatus(attempt)}</Link>
    : <span><span className='glyphicon glyphicon-refresh text-info' /> Pending</span>
}

SessionStatusView.propTypes = {
  session: React.PropTypes.object.isRequired
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
        this.setState({workflow})
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
          <td>{formatTimestamp(attempt.createdAt)}</td>
          <td>{formatSessionTime(attempt.sessionTime)}</td>
          <td>{formatDuration(attempt.createdAt, attempt.finishedAt)}</td>
          <td>{attemptStatus(attempt)}</td>
        </tr>
      )
    })

    return (
      <div className='row'>
        <h2>Attempts</h2>
        <div className='table-responsive'>
          <table className='table table-striped table-hover table-condensed'>
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
      return (
        <tr key={session.id}>
          <td><Link to={`/sessions/${session.id}`}>{session.id}</Link></td>
          <td><Link to={`/projects/${session.project.id}`}>{session.project.name}</Link></td>
          <td><MaybeWorkflowLink workflow={session.workflow} /></td>
          <td><SessionRevisionView session={session} /></td>
          <td>{formatSessionTime(session.sessionTime)}</td>
          <td>{session.lastAttempt ? formatTimestamp(session.lastAttempt.createdAt) : null}</td>
          <td>{session.lastAttempt ? formatDuration(session.lastAttempt.createdAt, session.lastAttempt.finishedAt) : null}</td>
          <td><SessionStatusView session={session} /></td>
        </tr>
      )
    })

    return (
      <div className='table-responsive'>
        <table className='table table-striped table-hover table-condensed'>
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
    projectId: number;
  };

  state: {
    schedules: [],
    loading: boolean
  };

  constructor (props) {
    super(props)
    this.state = {
      schedules: [],
      loading: false
    }
  }

  componentDidMount () {
    this.fetchSchedule()
  }

  fetchSchedule () {
    this.setState({loading: true})
    model().fetchProjectWorkflowSchedule(this.props.projectId, this.props.workflowName).then(schedules => {
      this.setState({
        schedules,
        loading: false
      })
    })
  }

  disableSchedule () {
    const { schedules } = this.state || {}
    this.setState({loading: true})
    model()
      .disableSchedule(schedules[0].id)
      .then(() => this.setState({loading: false}))
      .then(() => this.fetchSchedule())
  }

  enableSchedule () {
    const schedule = this.state.schedules[0]
    this.setState({loading: true})
    model()
      .enableSchedule(schedule.id)
      .then(() => this.setState({loading: false}))
      .then(() => this.fetchSchedule())
  }

  render () {
    const { schedules, loading } = this.state || {}
    const canPause = schedules && schedules.length && !schedules[0].disabledAt
    const rows = (schedules || []).map(schedule => {
      return (
        <tr key={schedule.id}>
          <td>{schedule.id}</td>
          <td>{schedule.revision}</td>
          <td><Link to={`/projects/${schedule.project.id}`}>{schedule.project.name}</Link></td>
          <td><Link to={`/workflows/${schedule.workflow.id}`}>{schedule.workflow.name}</Link></td>
          <td>{schedule.nextRunTime}</td>
          <td>{schedule.nextScheduleTime}</td>
          <td style={{ width: 60 }}>{statusButton}</td>
        </tr>
      )
    })
    const statusButton = canPause ? (
      <button
        className='btn btn-sm btn-secondary pull-right'
        onClick={this.disableSchedule.bind(this)}
      >
        PAUSE
      </button>
    ) : (
      <button
        className='btn btn-sm btn-success pull-right'
        onClick={this.enableSchedule.bind(this)}
      >
        RESUME
      </button>
    )
    const loadingLabel = <button disabled className='btn btn-sm btn-info pull-right'>LOADING ...</button>
    return (
      <div className='row'>
        <h2>
          Scheduling
          {loading ? loadingLabel : statusButton}
        </h2>
        <div className='table-responsive'>
          <table className='table table-striped table-hover table-condensed'>
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
      </div>
    )
  }
}

class ProjectsView extends React.Component {

  state = {
    projects: []
  };

  componentDidMount () {
    model().fetchProjects().then(projects => {
      this.setState({projects})
    })
  }

  render () {
    return (
      <div className='projects'>
        <h2>Projects</h2>
        <ProjectListView projects={this.state.projects} />
      </div>
    )
  }
}

class SessionsView extends React.Component {

  state = {
    sessions: []
  };

  componentDidMount () {
    model().fetchSessions().then(sessions => {
      this.setState({sessions})
    })
  }

  render () {
    return (
      <div>
        <h2>Sessions</h2>
        <SessionListView sessions={this.state.sessions} />
      </div>
    )
  }
}

class ProjectView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    projectId: number;
  };

  state = {
    project: {},
    workflows: [],
    sessions: [],
    archive: null
  };

  componentDidMount () {
    this.fetchProject()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetchProject()
  }

  fetchProject () {
    model().fetchProject(this.props.projectId).then(project => {
      if (!this.ignoreLastFetch) {
        this.setState({project: project})
      }
      return project
    }).then(project => {
      if (!this.ignoreLastFetch) {
        model().fetchProjectSessions(project.id).then(sessions => {
          if (!this.ignoreLastFetch) {
            this.setState({sessions})
          }
        })
      }
    })
    model().fetchProjectWorkflows(this.props.projectId).then(workflows => {
      if (!this.ignoreLastFetch) {
        this.setState({workflows: workflows})
      }
    })
  }

  render () {
    const project = this.state.project
    return (
      <div>
        <div className='row'>
          <h2>Project</h2>
          <table className='table table-condensed'>
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
                <td>{formatFullTimestamp(project.createdAt)}</td>
              </tr>
              <tr>
                <td>Updated</td>
                <td>{formatFullTimestamp(project.updatedAt)}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div className='row'>
          <h2>Workflows</h2>
          <WorkflowListView workflows={this.state.workflows} />
        </div>
        <div className='row'>
          <h2>Sessions</h2>
          <SessionListView sessions={this.state.sessions} />
        </div>
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
    model().fetchProjectWorkflowSessions(this.props.workflow.project.id, this.props.workflow.name).then(sessions => {
      if (!this.ignoreLastFetch) {
        this.setState({sessions})
      }
    })
    model().fetchProjectArchiveWithRevision(this.props.workflow.project.id, this.props.workflow.revision).then(projectArchive => {
      if (!this.ignoreLastFetch) {
        this.setState({projectArchive})
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

  render () {
    const wf = this.props.workflow
    return (
      <div>
        <div className='row'>
          <h2>Workflow</h2>
          <table className='table table-condensed'>
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
        <div className='row'>
          <h2>Definition</h2>
          <pre>
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
          </pre>
        </div>
        <div className='row'>
          <h2>Sessions</h2>
          <SessionListView sessions={this.state.sessions} />
        </div>
        <div className='row'>
          <h2>Files</h2>
          <WorkflowFilesView workflow={wf} projectArchive={this.state.projectArchive} />
        </div>
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
    const operators = ['td', 'td_load', 'sh', 'rb', 'py', 'mail']
    for (let operator of operators) {
      command = node[operator + '>'] || ''
      if (command) {
        taskType = operator
        break
      }
    }
  }
  return {taskType, command}
}

function resolveTaskFile (taskType:string, command:string, task:Object, projectArchive:ProjectArchive):?TaskFile {
  // TODO: resolve paths relative from the workflow file
  // TODO: make operators provide information about files used in a structured way instead of this hack
  const filename = path.normalize(command)
  if (!projectArchive.hasFile(filename)) {
    return null
  }
  const fileTypes = {
    'td': 'sql',
    'td_load': 'yaml',
    'sh': 'bash',
    'py': 'python',
    'rb': 'ruby',
    'mail': task['html'] ? 'html' : 'txt'
  }
  const fileType = fileTypes[taskType]
  if (!fileType) {
    return null
  }
  return {taskType, name: filename, fileType}
}

function enumerateTaskFiles (node:Object, files:Array<TaskFile>, projectArchive:ProjectArchive) {
  if (node && typeof node === 'object') {
    let {taskType, command} = task(node)
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

const FileView = (props:{file: string, fileType: string, contents: string}) =>
  <div>
    <h4>{props.file}</h4>
    <pre>
      <Measure>
        { ({ width }) =>
          <CodeViewer
            className='definition'
            language={props.fileType}
            value={props.contents}
            style={{ width }}
          />
        }
      </Measure>
    </pre>
  </div>

const WorkflowFilesView = (props:{workflow: Workflow, projectArchive: ?ProjectArchive}) =>
  props.projectArchive ? <div>{
    workflowFiles(props.workflow, props.projectArchive).map(file =>
      <FileView key={file.name} file={file.name} fileType={file.fileType}
        contents={fileString(file.name, props.projectArchive)} />)
  }</div> : null

class AttemptView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    attemptId: number;
  };

  state = {
    attempt: null
  };

  componentDidMount () {
    this.fetchAttempt()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetchAttempt()
  }

  fetchAttempt () {
    model().fetchAttempt(this.props.attemptId).then(attempt => {
      if (!this.ignoreLastFetch) {
        this.setState({attempt: attempt})
      }
    })
    model().fetchAttemptTasks(this.props.attemptId).then(tasks => {
      if (!this.ignoreLastFetch) {
        this.setState({tasks: tasks})
      }
    })
  }

  render () {
    const attempt = this.state.attempt

    if (!attempt) {
      return null
    }

    return (
      <div className='row'>
        <h2>Attempt</h2>
        <table className='table table-condensed'>
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
              <td>{formatSessionTime(attempt.sessionUuid)}</td>
            </tr>
            <tr>
              <td>Session Time</td>
              <td>{formatSessionTime(attempt.sessionTime)}</td>
            </tr>
            <tr>
              <td>Created</td>
              <td>{formatTimestamp(attempt.createdAt)}</td>
            </tr>
            <tr>
              <td>Status</td>
              <td>{attemptStatus(attempt)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    )
  }
}

const SessionView = withRouter(
  class PlainSessionView extends React.Component {

    props:{
      router: any;
      session: Session;
    };

    state = {
      loading: false
    };

    retrySession () {
      const { session, router } = this.props
      this.setState({ loading: true })
      model()
        .retrySession(session, uuid.v4())
        .then(() => router.push('/sessions'))
    }

    retrySessionWithLatestRevision () {
      const { session, router } = this.props
      this.setState({ loading: true })
      model()
        .retrySessionWithLatestRevision(session, uuid.v4())
        .then(() => router.push('/sessions'))
    }

    render () {
      const { loading } = this.state
      const { session } = this.props
      const { lastAttempt, project, workflow } = session
      const canRetry = attemptCanRetry(lastAttempt)
      return (
        <div className='row'>
          <h2>
            Session
            {canRetry &&
              <button
                className='btn btn-primary pull-right'
                disabled={loading}
                onClick={this.retrySession.bind(this)}
              >
                RETRY
              </button>
            }
            {canRetry &&
              <button
                className='btn btn-success pull-right'
                disabled={loading}
                onClick={this.retrySessionWithLatestRevision.bind(this)}
              >
                RETRY LATEST
              </button>
            }
          </h2>
          <table className='table table-condensed'>
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
                <td>{formatSessionTime(session.sessionTime)}</td>
              </tr>
              <tr>
                <td>Status</td>
                <td><SessionStatusView session={session} /></td>
              </tr>
              <tr>
                <td>Last Attempt</td>
                <td>{lastAttempt ? formatFullTimestamp(lastAttempt.createdAt) : null}</td>
              </tr>
              <tr>
                <td>Last Attempt Duration:</td>
                <td>{lastAttempt ? formatDuration(lastAttempt.createdAt, lastAttempt.finishedAt) : null}</td>
              </tr>
            </tbody>
          </table>
        </div>
      )
    }
  }
)

function formatSessionTime (t) {
  if (!t) {
    return ''
  }
  return <span>{t}</span>
}

function formatTimestamp (t) {
  if (!t) {
    return ''
  }
  const m = moment(t)
  return <span>{m.fromNow()}</span>
}

function formatFullTimestamp (t: ?string) {
  if (!t) {
    return ''
  }
  const m = moment(t)
  return <span>{t}<span className='text-muted'> ({m.fromNow()})</span></span>
}

function formatDuration (startTime: ?string, endTime: ?string) {
  if (!startTime || !endTime) {
    return ''
  }
  const duration = new Duration(new Date(startTime), new Date(endTime)).toString(1, 1) // format: 10y 2m 6d 3h 23m 8s
  return <span>{duration}</span>
}

const ParamsView = (props:{params: Object}) =>
  _.isEmpty(props.params)
    ? null
    : <CodeViewer className='params-view' language='yaml' value={yaml.safeDump(props.params, {sortKeys: true})} />

function formatTaskState (state) {
  switch (state) {

    // Pending
    case 'blocked':
      return <span><span className='glyphicon glyphicon-refresh text-info' /> Blocked</span>
    case 'ready':
      return <span><span className='glyphicon glyphicon-refresh text-info' /> Ready</span>
    case 'retry_waiting':
      return <span><span className='glyphicon glyphicon-refresh text-info' /> Retry Waiting</span>
    case 'group_retry_waiting':
      return <span><span className='glyphicon glyphicon-refresh text-info' /> Group Retry Waiting</span>
    case 'planned':
      return <span><span className='glyphicon glyphicon-refresh text-info' /> Planned</span>

    // Running
    case 'running':
      return <span><span className='glyphicon glyphicon-play text-info' /> Running</span>

    // Error
    case 'group_error':
      return <span><span className='glyphicon glyphicon-exclamation-sign text-danger' /> Group Error</span>
    case 'error':
      return <span><span className='glyphicon glyphicon-exclamation-sign text-danger' /> Error</span>

    // Warning
    case 'canceled':
      return <span><span className='glyphicon glyphicon-exclamation-sign text-warning' /> Canceled</span>

    // Success
    case 'success':
      return <span><span className='glyphicon glyphicon-ok text-success' /> Success</span>

    default:
      return <span>{_.capitalize(state)}</span>
  }
}

const JobLink = ({storeParams, stateParams}:{storeParams: Object, stateParams: Object}) => {
  const paramsJobId = storeParams.td && storeParams.td.last_job_id
  const stateJobId = stateParams.job && stateParams.job.jobId
  const jobId = paramsJobId || stateJobId
  const link = DIGDAG_CONFIG.td.jobUrl(jobId)
  if (!jobId) {
    return null
  }
  return <a href={link} target='_blank'>{jobId}</a>
}

const TaskListView = (props:{tasks: Array<Task>}) =>
  <div className='table-responsive'>
    <table className='table table-striped table-hover table-condensed'>
      <thead>
        <tr>
          <th>ID</th>
          <th>Job</th>
          <th>Name</th>
          <th>Parent ID</th>
          <th>Updated</th>
          <th>State</th>
          <th>Retry</th>
          <th>State Params</th>
          <th>Store Params</th>
        </tr>
      </thead>
      <tbody>
        {
          props.tasks.map(task =>
            <tr key={task.id}>
              <td>{task.id}</td>
              <td><JobLink storeParams={task.storeParams} stateParams={task.stateParams} /></td>
              <td>{task.fullName}</td>
              <td>{task.parentId}</td>
              <td>{formatTimestamp(task.updatedAt)}</td>
              <td>{formatTaskState(task.state)}</td>
              <td>{formatTimestamp(task.retryAt)}</td>
              <td><ParamsView params={task.stateParams} /></td>
              <td><ParamsView params={task.storeParams} /></td>
            </tr>
          )
        }
      </tbody>
    </table>
  </div>

class AttemptTasksView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    attemptId: number;
  };

  state = {
    tasks: []
  };

  componentDidMount () {
    this.fetchTasks()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetchTasks()
  }

  fetchTasks () {
    model().fetchAttemptTasks(this.props.attemptId).then(tasks => {
      if (!this.ignoreLastFetch) {
        this.setState({tasks})
      }
    })
  }

  render () {
    return (
      <div className='row'>
        <h2>Tasks</h2>
        <TaskListView tasks={this.state.tasks} />
      </div>
    )
  }
}

class LogFileView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    attemptId: number;
    file: LogFileHandle;
  };

  state = {
    data: ''
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

  fetchFile () {
    model().fetchLogFile(this.props.attemptId, this.props.file).then(data => {
      if (!this.ignoreLastFetch) {
        this.setState({data})
      }
    }, error => console.log(error))
  }

  render () {
    return this.state.data
      ? <span>{pako.inflate(this.state.data, {to: 'string'})}</span>
      : null
  }
}

class AttemptLogsView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    attemptId: number;
  };

  state = {
    files: []
  };

  componentDidMount () {
    this.fetchLogs()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetchLogs()
  }

  fetchLogs () {
    model().fetchAttemptLogFileHandles(this.props.attemptId).then(files => {
      if (!this.ignoreLastFetch) {
        const sortedFiles = _.sortBy(files, 'fileTime')
        this.setState({ files: sortedFiles })
      }
    })
  }

  logFiles () {
    if (!this.state.files.length) {
      return <pre />
    }
    return this.state.files.map(file =>
      <LogFileView key={file.fileName} file={file} attemptId={this.props.attemptId} />
    )
  }

  render () {
    return (
      <div className='row'>
        <h2>Logs</h2>
        <pre>{this.logFiles()}</pre>
      </div>
    )
  }
}

class VersionView extends React.Component {
  state = {
    version: ''
  };

  componentDidMount () {
    const url = DIGDAG_CONFIG.url + 'version'
    fetch(url).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      return response.json()
    }).then(version => {
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
  static contextTypes = {
    router: React.PropTypes.object
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
      ? <a className='navbar-brand' href='/' style={{marginTop: '-7px'}}><img src={navbar.logo} width='36' height='36' /></a>
      : null
  }

  className () {
    const navbar = DIGDAG_CONFIG.navbar
    return navbar && navbar.className ? navbar.className : 'navbar-inverse'
  }

  style () {
    const navbar = DIGDAG_CONFIG.navbar
    return navbar && navbar.style ? navbar.style : {}
  }

  isActiveClass (path) {
    const { router } = this.context
    return router.isActive(path) ? 'active' : ''
  }

  render () {
    return (
      <nav className={`navbar ${this.className()} navbar-fixed-top`} style={this.style()}>
        <div className='container-fluid'>
          <div className='navbar-header'>
            <button type='button' className='navbar-toggle collapsed' data-toggle='collapse' data-target='#navbar'
              aria-expanded='false' aria-controls='navbar'>
              <span className='sr-only'>Toggle navigation</span>
              <span className='icon-bar' />
              <span className='icon-bar' />
              <span className='icon-bar' />
            </button>
            {this.logo()}
            <a className='navbar-brand' href='/'>{this.brand()}</a>
          </div>
          <div id='navbar' className='collapse navbar-collapse'>
            <ul className='nav navbar-nav'>
              <li className={this.isActiveClass('/')}><Link to='/'>Workflows</Link></li>
              <li className={this.isActiveClass('/projects')}><Link to='/projects'>Projects</Link></li>
            </ul>
            <ul className='nav navbar-nav navbar-right'>
              <li><a href='/' onClick={this.logout}><span className='glyphicon glyphicon-log-out'
                aria-hidden='true' /> Logout</a></li>
            </ul>
            <p className='navbar-text navbar-right'><VersionView /></p>
          </div>
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
    <WorkflowsView />
    <SessionsView />
  </div>

const ProjectPage = (props:{params: {projectId: string}}) =>
  <div className='container-fluid'>
    <ProjectView projectId={parseInt(props.params.projectId)} />
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
    model().fetchProjectWorkflow(parseInt(this.props.params.projectId), this.props.params.workflowName).then(workflow => {
      if (!this.ignoreLastFetch) {
        this.setState({workflow})
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
    model().fetchWorkflow(parseInt(this.props.params.workflowId)).then(workflow => {
      if (!this.ignoreLastFetch) {
        this.setState({workflow})
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

const AttemptPage = (props:{params: {attemptId: string}}) =>
  <div className='container-fluid'>
    <AttemptView attemptId={parseInt(props.params.attemptId)} />
    <AttemptTasksView attemptId={parseInt(props.params.attemptId)} />
    <AttemptLogsView attemptId={parseInt(props.params.attemptId)} />
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
    tasks: Array<Task>;
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
    this.fetchSession()
  }

  componentWillUnmount () {
    this.ignoreLastFetch = true
  }

  componentDidUpdate (prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return
    }
    this.fetchSession()
  }

  fetchSession () {
    model().fetchSession(parseInt(this.props.params.sessionId)).then(session => {
      if (!this.ignoreLastFetch) {
        this.setState({session})
      }
    })
    model().fetchSessionAttempts(parseInt(this.props.params.sessionId)).then(attempts => {
      if (!this.ignoreLastFetch) {
        this.setState({attempts})
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
        {this.tasks()}
        {this.logs()}
        {this.attempts()}
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
      const value:string = scrub({key, value: this.state[key]})
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
      <div className='container'>
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
    model().fetchWorkflows().then(workflows => {
      this.setState({workflows})
    })
  }

  render () {
    return (
      <div className='workflows'>
        <h2>Workflows</h2>
        <WorkflowListView workflows={this.state.workflows} />
      </div>
    )
  }
}

class NotFoundPage extends React.Component {
  props:{
    router: Object;
  };

  componentDidMount () {
    this.props.router.replace('/')
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
      <div className='container'>
        <CodeViewer className='definition' language='digdag' value={this.definition()} />
      </div>
    )
  }
}

class AppWrapper extends React.Component {
  render () {
    return (
      <div className='container-fluid'>
        <Navbar />
        {this.props.children}
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
        <Router history={browserHistory}>
          <Route component={CacheLoader}>
            <Route component={AppWrapper}>
              <Route path='/' component={WorkflowsPage} />
              <Route path='/projects' component={ProjectsPage} />
              <Route path='/projects/:projectId' component={ProjectPage} />
              <Route path='/projects/:projectId/workflows/:workflowName' component={WorkflowPage} />
              <Route path='/workflows/:workflowId' component={WorkflowRevisionPage} />
              <Route path='/sessions/:sessionId' component={SessionPage} />
              <Route path='/attempts/:attemptId' component={AttemptPage} />
              {isDevelopmentEnv &&
                <Route>
                  <Route path='/parser-test' component={ParserTest} />
                  <Route path='/codeviewer' component={CodeViewerTest} />
                </Route>
              }
              <Route path='*' component={withRouter(NotFoundPage)} />
            </Route>
          </Route>
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
      this.state = {authenticated: true}
      this.setup({})
    } else {
      const credentials = window.localStorage.getItem('digdag.credentials')
      if (credentials) {
        this.setup(JSON.parse(credentials))
        this.state = {authenticated: true}
      } else {
        this.state = {authenticated: false}
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
    this.setState({authenticated: true})
  };

  render () {
    if (this.state.authenticated) {
      return <ConsolePage />
    } else {
      return <LoginPage onSubmit={this.handleCredentialsSubmit} />
    }
  }
}
