// @flow

import './style.less';

import "babel-polyfill";
import 'whatwg-fetch'

import _ from 'lodash-fp';

import React from 'react';
import {Router, Link, Route, browserHistory} from 'react-router';
import moment from 'moment';
import pako from 'pako';
import path from 'path';

//noinspection ES6UnusedImports
import Prism from 'prismjs';
import 'prismjs/components/prism-yaml';
import 'prismjs/components/prism-sql';
import 'prismjs/components/prism-bash';
import 'prismjs/components/prism-python';
import 'prismjs/components/prism-ruby';
import 'prismjs/components/prism-javascript';
import 'prismjs/themes/prism.css';
import {PrismCode} from "react-prism";

import type {
  HeadersProvider,
  ProjectArchive,
  Project,
  Workflow,
  Session,
  LogFileHandle,
  Credentials,
  Attempt,
  Task
} from "./model";
import {model, setup as setupModel} from "./model";

type Scrubber = (args:{key: string, value: string}) => string;

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

type ConsoleConfig = {
  url: string;
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

class ProjectListView extends React.Component {
  props:{
    projects: Array<Project>;
  };

  render() {
    const projectRows = this.props.projects.map(project =>
      <tr key={project.id}>
        <td><Link to={`/projects/${project.id}`}>{project.name}</Link></td>
        <td>{formatTimestamp(project.updatedAt)}</td>
        <td>{project.revision}</td>
      </tr>
    );
    return (
      <div className="table-responsive">
        <table className="table table-striped table-hover table-condensed">
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
    );
  }
}

class WorkflowListView extends React.Component {
  props:{
    workflows: Array<Workflow>;
  };

  render() {
    const rows = this.props.workflows.map(workflow =>
      <tr key={workflow.id}>
        <td><Link
          to={`/projects/${workflow.project.id}/workflows/${encodeURIComponent(workflow.name)}`}>{workflow.name}</Link>
        </td>
        <td>{workflow.revision}</td>
      </tr>
    );
    return (
      <div className="table-responsive">
        <table className="table table-striped table-hover table-condensed">
          <thead>
          <tr>
            <th>Name</th>
            <th>Revision</th>
          </tr>
          </thead>
          <tbody>
          {rows}
          </tbody>
        </table>
      </div>
    );
  }
}

function attemptStatus(attempt) {
  if (attempt.done) {
    if (attempt.success) {
      return <span><span className="glyphicon glyphicon-ok text-success"></span> Success</span>;
    } else {
      return <span><span className="glyphicon glyphicon-exclamation-sign text-danger"></span> Failure</span>;
    }
  } else {
    if (attempt.cancelRequested) {
      return <span><span className="glyphicon glyphicon-exclamation-sign text-warning"></span> Canceling</span>;
    } else {
      return <span><span className="glyphicon glyphicon-refresh text-info"></span> Pending</span>;
    }
  }
}

const SessionStatusView = (props:{session: Session}) => {
  const attempt = props.session.lastAttempt;
  return attempt
    ? <Link to={`/attempts/${attempt.id}`}>{attemptStatus(attempt)}</Link>
    : <span><span className="glyphicon glyphicon-refresh text-info"></span> Pending</span>;
};

SessionStatusView.propTypes = {
  session: React.PropTypes.object.isRequired,
};

class SessionRevisionView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    session: Session;
  };

  state = {
    workflow: null,
  };

  componentDidMount() {
    this.fetchWorkflow();
  }

  componentWillUnmount() {
    this.ignoreLastFetch = true;
  }

  componentDidUpdate(prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return;
    }
    this.fetchWorkflow();
  }

  fetchWorkflow() {
    const id = this.props.session.workflow.id;
    if (!id) {
      return;
    }
    model().fetchWorkflow(id).then(workflow => {
      if (!this.ignoreLastFetch) {
        this.setState({workflow});
      }
    });
  }

  render() {
    return this.state.workflow
      ? <span>{this.state.workflow.revision}</span>
      : <span></span>;
  }
}

class AttemptListView extends React.Component {

  props:{
    attempts: Array<Attempt>;
  };

  render() {
    const rows = this.props.attempts.map(attempt => {
      return (
        <tr key={attempt.id}>
          <td><Link to={`/attempts/${attempt.id}`}>{attempt.id}</Link></td>
          <td><Link to={`/workflows/${attempt.workflow.id}`}>{attempt.workflow.name}</Link></td>
          <td>{formatTimestamp(attempt.createdAt)}</td>
          <td>{formatSessionTime(attempt.sessionTime)}</td>
          <td>{attemptStatus(attempt)}</td>
        </tr>
      );
    });

    return (
      <div className="row">
        <h2>Attempts</h2>
        <div className="table-responsive">
          <table className="table table-striped table-hover table-condensed">
            <thead>
            <tr>
              <th>ID</th>
              <th>Workflow</th>
              <th>Created</th>
              <th>Session Time</th>
              <th>Status</th>
            </tr>
            </thead>
            <tbody>
            {rows}
            </tbody>
          </table>
        </div>
      </div>
    );
  }
}

class SessionListView extends React.Component {

  props:{
    sessions: Array<Session>;
  };

  render() {
    const rows = this.props.sessions.map(session => {
      return (
        <tr key={session.id}>
          <td><Link to={`/sessions/${session.id}`}>{session.id}</Link></td>
          <td><Link to={`/projects/${session.project.id}`}>{session.project.name}</Link></td>
          <td><Link to={`/workflows/${session.workflow.id}`}>{session.workflow.name}</Link></td>
          <td><SessionRevisionView session={session}/></td>
          <td>{formatSessionTime(session.sessionTime)}</td>
          <td>{session.lastAttempt ? formatTimestamp(session.lastAttempt.createdAt) : null}</td>
          <td><SessionStatusView session={session}/></td>
        </tr>
      );
    });

    return (
      <div className="table-responsive">
        <table className="table table-striped table-hover table-condensed">
          <thead>
          <tr>
            <th>ID</th>
            <th>Project</th>
            <th>Workflow</th>
            <th>Revision</th>
            <th>Session Time</th>
            <th>Last Attempt</th>
            <th>Status</th>
          </tr>
          </thead>
          <tbody>
          {rows}
          </tbody>
        </table>
      </div>
    );
  }
}

class ProjectsView extends React.Component {

  state = {
    projects: [],
  };

  componentDidMount() {
    model().fetchProjects().then(projects => {
      this.setState({projects});
    });
  }

  render() {
    return (
      <div className="projects">
        <h2>Projects</h2>
        <ProjectListView projects={this.state.projects}/>
      </div>
    );
  }
}

class SessionsView extends React.Component {

  state = {
    sessions: [],
  };

  componentDidMount() {
    model().fetchSessions().then(sessions => {
      this.setState({sessions});
    });
  }

  render() {
    return (
      <div>
        <h2>Sessions</h2>
        <SessionListView sessions={this.state.sessions}/>
      </div>
    );
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
    archive: null,
  };

  componentDidMount() {
    this.fetchProject();
  }

  componentWillUnmount() {
    this.ignoreLastFetch = true;
  }

  componentDidUpdate(prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return;
    }
    this.fetchProject()
  }

  fetchProject() {
    model().fetchProject(this.props.projectId).then(project => {
      if (!this.ignoreLastFetch) {
        this.setState({project: project});
      }
      return project;
    }).then(project => {
      if (!this.ignoreLastFetch) {
        model().fetchProjectSessions(project.id).then(sessions => {
          if (!this.ignoreLastFetch) {
            this.setState({sessions});
          }
        });
      }
    });
    model().fetchProjectWorkflows(this.props.projectId).then(workflows => {
      if (!this.ignoreLastFetch) {
        this.setState({workflows: workflows});
      }
    });
  }

  render() {
    const project = this.state.project;
    return (
      <div>
        <div className="row">
          <h2>Project</h2>
          <table className="table table-condensed">
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
        <div className="row">
          <h2>Workflows</h2>
          <WorkflowListView workflows={this.state.workflows}/>
        </div>
        <div className="row">
          <h2>Sessions</h2>
          <SessionListView sessions={this.state.sessions}/>
        </div>
      </div>
    );

  }
}

class WorkflowView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    workflow: Workflow;
  };

  state = {
    sessions: [],
    projectArchive: null,
  };

  componentDidMount() {
    this.fetchWorkflow();
  }

  componentWillUnmount() {
    this.ignoreLastFetch = true;
  }

  componentDidUpdate(prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return;
    }
    this.fetchWorkflow();
  }

  fetchWorkflow() {
    model().fetchProjectWorkflowSessions(this.props.workflow.project.id, this.props.workflow.name).then(sessions => {
      if (!this.ignoreLastFetch) {
        this.setState({sessions});
      }
    });
    model().fetchProjectArchiveWithRevision(this.props.workflow.project.id, this.props.workflow.revision).then(projectArchive => {
      if (!this.ignoreLastFetch) {
        this.setState({projectArchive});
      }
    });
  }

  definition() {
    if (!this.state.projectArchive) {
      return '';
    }
    const workflow = this.state.projectArchive.getWorkflow(this.props.workflow.name);
    if (!workflow) {
      return '';
    }
    return workflow.trim();
  }

  render() {
    const wf = this.props.workflow;
    return (
      <div>
        <div className="row">
          <h2>Workflow</h2>
          <table className="table table-condensed">
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
        <div className="row">
          <h2>Definition</h2>
          <pre><PrismCode className="language-yaml">{this.definition()}</PrismCode></pre>
        </div>
        <div className="row">
          <h2>Sessions</h2>
          <SessionListView sessions={this.state.sessions}/>
        </div>
        <div className="row">
          <h2>Files</h2>
          <WorkflowFilesView workflow={wf} projectArchive={this.state.projectArchive}/>
        </div>
      </div>
    );
  }
}

type TaskFile = {
  name: string;
  taskType: string;
  fileType: string;
};

function task(node:Object) {
  let command = '';
  let taskType = node['_type'] || '';
  if (taskType) {
    command = node['_command'];
  } else {
    const operators = ['td', 'td_load', 'sh', 'rb', 'py', 'mail'];
    for (let operator of operators) {
      command = node[operator + '>'] || '';
      if (command) {
        taskType = operator;
        break;
      }
    }
  }
  return {taskType, command};
}

function resolveTaskFile(taskType:string, command:string, task:Object, projectArchive:ProjectArchive):?TaskFile {
  // TODO: resolve paths relative from the workflow file
  // TODO: make operators provide information about files used in a structured way instead of this hack
  const filename = path.normalize(command);
  if (!projectArchive.hasFile(filename)) {
    return null;
  }
  const fileTypes = {
    'td': 'sql',
    'td_load': 'yaml',
    'sh': 'bash',
    'py': 'python',
    'rb': 'ruby',
    'mail': task['html'] ? 'html' : 'txt',
  };
  const fileType = fileTypes[taskType];
  if (!fileType) {
    return null;
  }
  return {taskType, name: filename, fileType};
}

function enumerateTaskFiles(node:Object, files:Array<TaskFile>, projectArchive:ProjectArchive) {
  if (node.constructor == Object) {
    let {taskType, command} = task(node);
    const taskFile = resolveTaskFile(taskType, command, node, projectArchive);
    if (taskFile) {
      files.push(taskFile);
    } else {
      for (let key of Object.keys(node)) {
        enumerateTaskFiles(node[key], files, projectArchive);
      }
    }
  }
}

function workflowFiles(workflow:Workflow, projectArchive:ProjectArchive):Array<TaskFile> {
  const files = [];
  enumerateTaskFiles(workflow.config, files, projectArchive);
  console.log('workflowFiles', 'workflow', workflow, 'projectArchive', projectArchive, 'files', files);
  return files;
}

function fileString(file:string, projectArchive:?ProjectArchive) {
  if (!projectArchive) {
    return '';
  }
  const buffer = projectArchive.getFileContents(file);
  if (!buffer) {
    return '';
  }
  return buffer.toString()
}

const FileView = (props:{file: string, fileType: string, contents: string}) =>
  <div>
    <h4>{props.file}</h4>
    <pre><PrismCode className={`language-${props.fileType}`}>{props.contents}</PrismCode></pre>
  </div>;

const WorkflowFilesView = (props:{workflow: Workflow, projectArchive: ?ProjectArchive}) =>
  props.projectArchive ? <div>{
    workflowFiles(props.workflow, props.projectArchive).map(file =>
      <FileView key={file.name} file={file.name} fileType={file.fileType}
                contents={fileString(file.name, props.projectArchive)}/>)
  }</div> : null;

class AttemptView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    attemptId: number;
  };

  state = {
    attempt: null,
  };

  componentDidMount() {
    this.fetchAttempt();
  }

  componentWillUnmount() {
    this.ignoreLastFetch = true;
  }

  componentDidUpdate(prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return;
    }
    this.fetchAttempt()
  }

  fetchAttempt() {
    model().fetchAttempt(this.props.attemptId).then(attempt => {
      if (!this.ignoreLastFetch) {
        this.setState({attempt: attempt});
      }
    });
    model().fetchAttemptTasks(this.props.attemptId).then(tasks => {
      if (!this.ignoreLastFetch) {
        this.setState({tasks: tasks});
      }
    });
  }

  render() {
    const attempt = this.state.attempt;

    if (!attempt) {
      return null;
    }

    return (
      <div className="row">
        <h2>Attempt</h2>
        <table className="table table-condensed">
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
    );
  }
}

const SessionView = (props:{session: Session}) =>
  <div className="row">
    <h2>Session</h2>
    <table className="table table-condensed">
      <tbody>
      <tr>
        <td>ID</td>
        <td>{props.session.id}</td>
      </tr>
      <tr>
        <td>Project</td>
        <td><Link to={`/projects/${props.session.project.id}`}>{props.session.project.name}</Link></td>
      </tr>
      <tr>
        <td>Workflow</td>
        <td><Link to={`/workflows/${props.session.workflow.id}`}>{props.session.workflow.name}</Link></td>
      </tr>
      <tr>
        <td>Revision</td>
        <td><SessionRevisionView session={props.session}/></td>
      </tr>
      <tr>
        <td>Session UUID</td>
        <td>{props.session.sessionUuid}</td>
      </tr>
      <tr>
        <td>Session Time</td>
        <td>{formatSessionTime(props.session.sessionTime)}</td>
      </tr>
      <tr>
        <td>Status</td>
        <td><SessionStatusView session={props.session}/></td>
      </tr>
      <tr>
        <td>Last Attempt</td>
        <td>{props.session.lastAttempt ? formatFullTimestamp(props.session.lastAttempt.createdAt) : null}</td>
      </tr>
      </tbody>
    </table>
  </div>;

function formatSessionTime(t) {
  if (!t) {
    return '';
  }
  return <span>{t}</span>;
}

function formatTimestamp(t) {
  if (!t) {
    return '';
  }
  const m = moment(t);
  return <span>{m.fromNow()}</span>;
}

function formatFullTimestamp(t) {
  if (!t) {
    return '';
  }
  const m = moment(t);
  return <span>{t}<span className="text-muted"> ({m.fromNow()})</span></span>;
}

const TaskListView = (props:{tasks: Array<Task>}) =>
  <div className="table-responsive">
    <table className="table table-striped table-hover table-condensed">
      <thead>
      <tr>
        <th>ID</th>
        <th>Name</th>
        <th>Parent ID</th>
        <th>Updated</th>
        <th>State</th>
        <th>Retry</th>
      </tr>
      </thead>
      <tbody>
      {
        props.tasks.map(task =>
          <tr key={task.id}>
            <td>{task.id}</td>
            <td>{task.fullName}</td>
            <td>{task.parentId}</td>
            <td>{formatTimestamp(task.updatedAt)}</td>
            <td>{task.state}</td>
            <td>{formatTimestamp(task.retryAt)}</td>
          </tr>
        )
      }
      </tbody>
    </table>
  </div>;


class AttemptTasksView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    attemptId: number;
  };

  state = {
    tasks: [],
  };

  componentDidMount() {
    this.fetchTasks();
  }

  componentWillUnmount() {
    this.ignoreLastFetch = true;
  }

  componentDidUpdate(prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return;
    }
    this.fetchTasks()
  }

  fetchTasks() {
    model().fetchAttemptTasks(this.props.attemptId).then(tasks => {
      if (!this.ignoreLastFetch) {
        this.setState({tasks});
      }
    });
  }

  render() {
    return (
      <div className="row">
        <h2>Tasks</h2>
        <TaskListView tasks={this.state.tasks}/>
      </div>
    );
  }
}

class LogFileView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    file: LogFileHandle;
  };

  state = {
    data: '',
  };

  componentDidMount() {
    this.fetchFile();
  }

  componentWillUnmount() {
    this.ignoreLastFetch = true;
  }

  componentDidUpdate(prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return;
    }
    this.fetchFile()
  }

  fetchFile() {
    model().fetchLogFile(this.props.file).then(data => {
      if (!this.ignoreLastFetch) {
        this.setState({data});
      }
    }, error => console.log(error));
  }

  render() {
    return this.state.data
      ? <pre>{pako.inflate(this.state.data, {to: 'string'})}</pre>
      : <pre></pre>;
  }
}


class AttemptLogsView extends React.Component {
  ignoreLastFetch:boolean;

  props:{
    attemptId: number;
  };

  state = {
    files: [],
  };

  componentDidMount() {
    this.fetchLogs();
  }

  componentWillUnmount() {
    this.ignoreLastFetch = true;
  }

  componentDidUpdate(prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return;
    }
    this.fetchLogs()
  }

  fetchLogs() {
    model().fetchAttemptLogFileHandles(this.props.attemptId).then(files => {
      if (!this.ignoreLastFetch) {
        this.setState({files});
      }
    });
  }

  logFiles() {
    if (this.state.files.length == 0) {
      return <pre></pre>;
    }
    return this.state.files.map(file => {
      return <LogFileView key={file.fileName} file={file}/>;
    });
  }

  render() {
    return (
      <div className="row">
        <h2>Logs</h2>
        {this.logFiles()}
      </div>
    );
  }
}

class VersionView extends React.Component {
  state = {
    version: '',
  };

  componentDidMount() {
    const url = DIGDAG_CONFIG.url + 'version';
    fetch(url).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText);
      }
      return response.json();
    }).then(version => {
      this.setState(version);
    });
  }

  render() {
    return (
      <span>{this.state.version}</span>
    );
  }
}

class Navbar extends React.Component {
  logout(e) {
    e.preventDefault();
    window.localStorage.removeItem("digdag.credentials");
    window.location = '/';
  }

  logo() {
    const navbar = DIGDAG_CONFIG.navbar;
    return navbar && navbar.logo
      ? <a className="navbar-brand" href="/" style={{marginTop: '-7px'}}><img src={navbar.logo} width="36" height="36"></img></a>
      : null;
  }

  className() {
    const navbar = DIGDAG_CONFIG.navbar;
    return navbar && navbar.className ? navbar.className : 'navbar-inverse';
  }

  style() {
    const navbar = DIGDAG_CONFIG.navbar;
    return navbar && navbar.style ? navbar.style : {};
  }

  render() {
    return (
      <nav className={`navbar ${this.className()} navbar-fixed-top`} style={this.style()}>
        <div className="container-fluid">
          <div className="navbar-header">
            <button type="button" className="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar"
                    aria-expanded="false" aria-controls="navbar">
              <span className="sr-only">Toggle navigation</span>
              <span className="icon-bar"></span>
              <span className="icon-bar"></span>
              <span className="icon-bar"></span>
            </button>
            {this.logo()}
            <a className="navbar-brand" href="/">{DIGDAG_CONFIG.brand ? DIGDAG_CONFIG.brand : 'Digdag'}</a>
          </div>
          <div id="navbar" className="collapse navbar-collapse">
            <ul className="nav navbar-nav">
              <li className="active"><a href="/">Projects</a></li>
            </ul>
            <ul className="nav navbar-nav navbar-right">
              <li><a href="/" onClick={this.logout}><span className="glyphicon glyphicon-log-out"
                                                          aria-hidden="true"></span> Logout</a></li>
            </ul>
            <p className="navbar-text navbar-right"><VersionView /></p>
          </div>
        </div>
      </nav>
    );
  }
}

const ProjectsPage = (props:{}) =>
  <div className="container-fluid">
    <Navbar />
    <ProjectsView />
    <SessionsView />
  </div>;

const ProjectPage = (props:{params: {projectId: string}}) =>
  <div className="container-fluid">
    <Navbar />
    <ProjectView projectId={parseInt(props.params.projectId)}/>
  </div>;

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

  constructor(props) {
    super(props);
    this.state = {
      workflow: null,
    };
  }

  componentDidMount() {
    this.fetchWorkflow();
  }

  componentWillUnmount() {
    this.ignoreLastFetch = true;
  }

  componentDidUpdate(prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return;
    }
    this.fetchWorkflow();
  }

  fetchWorkflow() {
    model().fetchProjectWorkflow(parseInt(this.props.params.projectId), this.props.params.workflowName).then(workflow => {
      if (!this.ignoreLastFetch) {
        this.setState({workflow});
      }
    });
  }

  workflow() {
    return this.state.workflow ? <WorkflowView workflow={this.state.workflow}/> : null;
  }

  render() {
    return (
      <div className="container-fluid">
        <Navbar />
        {this.workflow()}
      </div>
    );
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

  constructor(props) {
    super(props);
    this.state = {
      workflow: null,
    };
  }

  componentDidMount() {
    this.fetchWorkflow();
  }

  componentWillUnmount() {
    this.ignoreLastFetch = true;
  }

  componentDidUpdate(prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return;
    }
    this.fetchWorkflow();
  }

  fetchWorkflow() {
    model().fetchWorkflow(parseInt(this.props.params.workflowId)).then(workflow => {
      if (!this.ignoreLastFetch) {
        this.setState({workflow});
      }
    });
  }

  workflow() {
    return this.state.workflow ? <WorkflowView workflow={this.state.workflow}/> : null;
  }

  render() {
    return (
      <div className="container-fluid">
        <Navbar />
        {this.workflow()}
      </div>
    );
  }
}

const AttemptPage = (props:{params: {attemptId: string}}) =>
  <div className="container-fluid">
    <Navbar />
    <AttemptView attemptId={parseInt(props.params.attemptId)}/>
    <AttemptTasksView attemptId={parseInt(props.params.attemptId)}/>
    <AttemptLogsView attemptId={parseInt(props.params.attemptId)}/>
  </div>;

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

  constructor(props) {
    super(props);
    this.state = {
      session: null,
      tasks: [],
      attempts: [],
    };
  }

  componentDidMount() {
    this.fetchSession();
  }

  componentWillUnmount() {
    this.ignoreLastFetch = true;
  }

  componentDidUpdate(prevProps) {
    if (_.isEqual(prevProps, this.props)) {
      return;
    }
    this.fetchSession()
  }

  fetchSession() {
    model().fetchSession(parseInt(this.props.params.sessionId)).then(session => {
      if (!this.ignoreLastFetch) {
        this.setState({session});
      }
    });
    model().fetchSessionAttempts(parseInt(this.props.params.sessionId)).then(attempts => {
      if (!this.ignoreLastFetch) {
        this.setState({attempts});
      }
    });
  }

  session() {
    return this.state.session
      ? <SessionView session={this.state.session}/>
      : null;
  }

  tasks() {
    return this.state.session && this.state.session.lastAttempt
      ? <AttemptTasksView attemptId={this.state.session.lastAttempt.id}/>
      : null;
  }

  logs() {
    return this.state.session && this.state.session.lastAttempt
      ? <AttemptLogsView attemptId={this.state.session.lastAttempt.id}/>
      : <pre></pre>;
  }

  attempts() {
    return this.state.attempts
      ? <AttemptListView attempts={this.state.attempts}/>
      : null;
  }

  render() {
    return (
      <div className="container-fluid">
        <Navbar />
        {this.session()}
        {this.tasks()}
        {this.logs()}
        {this.attempts()}
      </div>
    );
  }
}

class LoginPage extends React.Component {

  props:{
    onSubmit: (credentials:Credentials) => void;
  };

  state:Credentials;

  constructor(props) {
    super(props);
    this.state = {};
    DIGDAG_CONFIG.auth.items.forEach(item => {
      this.state[item.key] = '';
    });
  }

  onChange(key) {
    return (e) => {
      e.preventDefault();
      const state = {};
      state[key] = e.target.value;
      this.setState(state);
    };
  }

  valid(credentials:Credentials, key:string, value:string) {
    return (key:string) => {
      credentials[key] = value;
      if (DIGDAG_CONFIG.auth.items.length == Object.keys(credentials).length) {
        this.props.onSubmit(credentials);
      }
    }
  }

  invalid(values, key, value, message) {
    return (key) => {
      console.log(`${key} is invalid: message=${message})`);
    };
  }

  handleSubmit = (e) => {
    e.preventDefault();
    const credentials:Credentials = {};
    for (let item of DIGDAG_CONFIG.auth.items) {
      const key = item.key;
      const scrub:Scrubber = item.scrub ? item.scrub : (args:{key: string, value: string}) => value;
      const value:string = scrub({key, value: this.state[key]});
      item.validate({
        key,
        value,
        valid: this.valid(credentials, key, value),
        invalid: this.invalid(credentials, key, key)
      });
    }
  };

  render() {
    const authItems = DIGDAG_CONFIG.auth.items.map(item => {
      return (
        <div className="form-group" key={item.key}>
          <label for={item.key}>{item.name}</label>
          <input
            type={item.type}
            className="form-control"
            onChange={this.onChange(item.key)}
            value={this.state[item.key]}
          />
        </div>
      );
    });

    return (
      <div className="container">
        <Navbar />
        <h1>{DIGDAG_CONFIG.auth.title}</h1>
        <form onSubmit={this.handleSubmit}>
          {authItems}
          <button type="submit" className="btn btn-default">Submit</button>
        </form>
      </div>
    );
  }
}

class ConsolePage extends React.Component {

  render() {
    return (
      <div className="container-fluid">
        <Router history={browserHistory}>
          <Route path="/" component={ProjectsPage}/>
          <Route path="/projects/:projectId" component={ProjectPage}/>
          <Route path="/projects/:projectId/workflows/:workflowName" component={WorkflowPage}/>
          <Route path="/workflows/:workflowId" component={WorkflowRevisionPage}/>
          <Route path="/sessions/:sessionId" component={SessionPage}/>
          <Route path="/attempts/:attemptId" component={AttemptPage}/>
        </Router>
      </div>
    );
  }
}

export default class Console extends React.Component {

  state:{
    authenticated: bool
  };

  constructor(props:any) {
    super(props);
    const credentials = window.localStorage.getItem("digdag.credentials");
    if (credentials) {
      this.setup(JSON.parse(credentials));
      this.state = {authenticated: true};
    } else {
      this.state = {authenticated: false};
    }
  }

  setup(credentials:Credentials) {
    setupModel({
      url: DIGDAG_CONFIG.url,
      credentials: credentials,
      headers: DIGDAG_CONFIG.headers
    });
  }

  handleCredentialsSubmit:(credentials:Credentials) => void = (credentials:Credentials) => {
    window.localStorage.setItem("digdag.credentials", JSON.stringify(credentials));
    this.setup(credentials);
    this.setState({authenticated: true});
  };

  render() {
    if (this.state.authenticated) {
      return <ConsolePage />;
    } else {
      return <LoginPage onSubmit={this.handleCredentialsSubmit}/>;
    }
  }
}
