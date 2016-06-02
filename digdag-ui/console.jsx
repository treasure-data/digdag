// @flow

import './style.less';

import React from 'react';
import {Router, DefaultRoute, Link, Route, RouteHandler, browserHistory} from 'react-router';
import moment from 'moment';
import jsyaml from 'js-yaml';

import "babel-polyfill";
import 'whatwg-fetch'
import pako from 'pako';
import untar from 'js-untar';

import Prism from 'prismjs';
import 'prismjs/components/prism-yaml';
import 'prismjs/themes/prism.css';
import {PrismCode} from "react-prism";

import _ from 'lodash-fp';

import {Buffer} from 'buffer/';

type Scrubber = (args: {key: string, value: string}) => string;

type AuthItem = {
  key: string;
  name: string;
  type: string;
  validate: (args: {
    key: string;
    value: string;
    valid: (key: string) => void;
    invalid: (key: string) => void;
  }) => void;
  scrub: Scrubber;
}

type Credentials = {[key: string]: string};
type Headers = {[key: string]: string};

type ConsoleConfig = {
  url: string;
  brand: string;
  auth: {
    title: string;
    items: Array<AuthItem>;
  };
  headers: (args: {credentials: Credentials}) => Headers;
}

declare var DIGDAG_CONFIG: ConsoleConfig;

type DirectDownloadHandle = {
  type: string;
  url: string;
}

type LogFile = {
  fileName: string;
  fileSize: number;
  taskName: string;
  fileTime: string;
  agentId: string;
  direct: ?DirectDownloadHandle;
}

type TarEntry = {
  name: string;
  buffer: ArrayBuffer;
}

type IdName = {
  id: number;
  name: string;
}

type NameOptionalId = {
  name: string;
  id: ?number;
};

type UUID = string;

type Workflow = {
  id: number;
  name: string;
  project: IdName;
  revision: string;
  config: Object;
};

type Project = {
  id: number;
  name: string;
  revision: string;
  createdAt: string;
  updatedAt: string;
  archiveType: string;
  archiveMd5: string;
}

type Task = {
  id: number;
  fullName: string;
  parentId: ?number;
  config: Object;
  upstreams: Array<number>;
  isGroup: boolean;
  state: string;
  exportParams: Object;
  storeParams: Object;
  stateParams: Object;
  updatedAt: string;
  retryAt: ?string;
};

type Attempt = {
  id: number;
  project: IdName;
  workflow: NameOptionalId;
  sessionId: number;
  sessionUuid: UUID;
  sessionTime: string;
  retryAttemptName: ?string;
  done: boolean;
  success: boolean;
  cancelRequested: boolean;
  params: Object;
  createdAt: string;
};

type Session = {
  id: number;
  project: IdName;
  workflow: NameOptionalId;
  sessionUuid: UUID;
  sessionTime: string;
  lastAttempt: ?{
    id: number;
    retryAttemptName: ?string;
    done: boolean;
    success: boolean;
    cancelRequested: boolean;
    params: Object;
    createdAt: string;
  };
};

declare function escape(buf: Uint8Array): string;

class ProjectArchive {
  files: Array<TarEntry>;
  fileMap: Map<string, TarEntry>;
  legacy: boolean;
  constructor(files: Array<TarEntry>) {
    this.files = files;
    this.fileMap = new Map();
    this.legacy = false;
    for (let file of files) {
      if (file.name == 'digdag.yml') {
        this.legacy = true;
      }
      this.fileMap.set(file.name, file);
    }
  }

  getWorkflow(name) {
    const suffix = this.legacy ? 'yml' : 'dig';
    const filename = `${name}.${suffix}`;
    const file = this.fileMap.get(filename);
    if (!file) {
      return null;
    }
    return new Buffer(file.buffer).toString();
  }
}

class Model {
  static INSTANCE: Model;
  url: string;
  credentials: any;

  constructor(url: string, credentials: any) {
    this.url = url;
    this.credentials = credentials;
  }

  fetchProjects(callbacks) {
    this.get(`projects/`, callbacks);
  }

  fetchProject(projectId, callbacks) {
    this.get(`projects/${projectId}`, callbacks);
  }

  fetchWorkflow(workflowId, callbacks) {
    this.get(`workflows/${workflowId}`, callbacks);
  }

  fetchProjectWorkflows(projectId, callbacks) {
    this.get(`projects/${projectId}/workflows`, callbacks);
  }

  fetchProjectWorkflow(projectId, workflowName, callbacks) {
    this.get(`projects/${projectId}/workflows/${encodeURIComponent(workflowName)}`, callbacks);
  }

  fetchProjectWorkflowAttempts(projectName, workflowName, callbacks) {
    this.get(`attempts?project=${encodeURIComponent(projectName)}&workflow=${encodeURIComponent(workflowName)}`, callbacks);
  }

  fetchProjectWorkflowSessions(projectId, workflowName, callbacks) {
    this.get(`projects/${projectId}/sessions?workflow=${encodeURIComponent(workflowName)}`, callbacks);
  }

  fetchProjectSessions(projectId, callbacks) {
    this.get(`projects/${projectId}/sessions`, callbacks);
  }

  fetchProjectAttempts(projectName, callbacks) {
    this.get(`attempts?project=${encodeURIComponent(projectName)}`, callbacks);
  }

  fetchAttempts(callbacks) {
    this.get(`attempts`, callbacks);
  }

  fetchSessions(callbacks) {
    this.get(`sessions`, callbacks);
  }

  fetchAttempt(attemptId, callbacks) {
    this.get(`attempts/${attemptId}`, callbacks);
  }

  fetchSession(sessionId, callbacks) {
    this.get(`sessions/${sessionId}`, callbacks);
  }

  fetchSessionAttempts(sessionId, callbacks) {
    this.get(`sessions/${sessionId}/attempts?include_retried=true`, callbacks);
  }

  fetchAttemptTasks(attemptId, callbacks) {
    this.get(`attempts/${attemptId}/tasks`, callbacks);
  }

  fetchAttemptLogFileHandles(attemptId, callbacks) {
    this.get(`logs/${attemptId}/files`, callbacks);
  }

  fetchAttemptTaskLogFileHandles(attemptId, taskName, callbacks) {
    this.get(`logs/${attemptId}/files?task=${encodeURIComponent(taskName)}`, callbacks);
  }

  fetchLogFile(file, callbacks) {
    if (!file.direct) {
      return;
    }
    fetch(file.direct.url).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText);
      }
      response.arrayBuffer().then(buffer => {
        callbacks['success'](buffer);
      });
    });
  }

  fetchProjectArchiveLatest(projectId, callbacks) {
    fetch(this.url + `projects/${projectId}/archive`, {
      headers: this.headers(),
    }).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText);
      }
      response.arrayBuffer().then(data => {
        untar(pako.inflate(data)).then(files => {
          const archive = new ProjectArchive((files: Array<TarEntry>));
          callbacks['success'](archive);
        });
      });
    });
  }

  fetchProjectArchiveWithRevision(projectId, revisionName, callbacks) {
    fetch(this.url + `projects/${projectId}/archive?revision=${encodeURIComponent(revisionName)}`, {
      headers: this.headers(),
    }).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText);
      }
      response.arrayBuffer().then(data => {
        untar(pako.inflate(data).buffer).then(files => {
          const archive = new ProjectArchive(files);
          callbacks['success'](archive);
        });
      });
    });
  }

  get(url, callbacks) {
    this.fetch('GET', url, 'json', callbacks);
  }

  fetch(type, url, dataType, callbacks) {
    fetch(this.url + url, {
      headers: this.headers(),
    }).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText);
      }
      return response.json();
    }).then(value => {
      if (callbacks.success) {
        callbacks.success(value);
      }
    });
  }

  headers() {
    return DIGDAG_CONFIG.headers({credentials: this.credentials});
  }
}

// TODO: figure out how to have this not be a singleton
function setupModel(credentials) {
  Model.INSTANCE = new Model(DIGDAG_CONFIG.url, credentials);
}

function model() {
  return Model.INSTANCE;
}

class ProjectListView extends React.Component {
  props: {
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
  props: {
    workflows: Array<Workflow>;
  };

  render() {
    const rows = this.props.workflows.map(workflow =>
      <tr key={workflow.id}>
        <td><Link to={`/projects/${workflow.project.id}/workflows/${encodeURIComponent(workflow.name)}`}>{workflow.name}</Link></td>
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

const SessionStatusView = (props: {session: Session}) => {
  const attempt = props.session.lastAttempt;
  return attempt
    ? <Link to={`/attempts/${attempt.id}`}>{attemptStatus(attempt)}</Link>
    : <span><span className="glyphicon glyphicon-refresh text-info"></span> Pending</span>;
};

SessionStatusView.propTypes = {
  session: React.PropTypes.object.isRequired,
};

class SessionRevisionView extends React.Component {
  ignoreLastFetch: boolean;

  props: {
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
    model().fetchWorkflow(this.props.session.workflow.id, {
      success: workflow => {
        if (!this.ignoreLastFetch) {
          this.setState({workflow});
        }
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

  props: {
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

  props: {
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
    model().fetchProjects({
      success: projects => {
        this.setState({projects});
      }
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
    model().fetchSessions({
      success: sessions => {
        this.setState({sessions});
      }
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
  ignoreLastFetch: boolean;

  props: {
    projectId: string;
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
    const oldId = prevProps.projectId;
    const newId = this.props.projectId;
    if (newId !== oldId) {
      this.fetchProject()
    }
  }

  fetchProject() {
    model().fetchProject(this.props.projectId, {
      success: project => {
        if (!this.ignoreLastFetch) {
          this.setState({project: project});
          model().fetchProjectSessions(this.state.project.id, {
            success: sessions => {
              if (!this.ignoreLastFetch) {
                this.setState({sessions});
              }
            }
          });
        }
      }
    });
    model().fetchProjectWorkflows(this.props.projectId, {
      success: workflows => {
        if (!this.ignoreLastFetch) {
          this.setState({workflows: workflows});
        }
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
  ignoreLastFetch: boolean;

  props: {
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
    model().fetchProjectWorkflowSessions(this.props.workflow.project.id, this.props.workflow.name, {
      success: sessions => {
        if (!this.ignoreLastFetch) {
          this.setState({sessions});
        }
      }
    });
    model().fetchProjectArchiveWithRevision(this.props.workflow.project.id, this.props.workflow.revision, {
      success: projectArchive => {
        if (!this.ignoreLastFetch) {
          this.setState({projectArchive});
        }
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
      </div>
    );
  }
}

class AttemptView extends React.Component {
  ignoreLastFetch: boolean;

  props: {
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
    const oldId = prevProps.attemptId;
    const newId = this.props.attemptId;
    if (newId !== oldId) {
      this.fetchAttempt()
    }
  }

  fetchAttempt() {
    model().fetchAttempt(this.props.attemptId, {
      success: attempt => {
        if (!this.ignoreLastFetch) {
          this.setState({attempt: attempt});
        }
      }
    });
    model().fetchAttemptTasks(this.props.attemptId, {
      success: tasks => {
        if (!this.ignoreLastFetch) {
          this.setState({tasks: tasks});
        }
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

const SessionView = (props: {session: Session}) =>
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

const TaskListView = (props: {tasks: Array<Task>}) =>
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
  ignoreLastFetch: boolean;

  props: {
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
    const oldId = prevProps.attemptId;
    const newId = this.props.attemptId;
    if (newId !== oldId) {
      this.fetchTasks()
    }
  }

  fetchTasks() {
    model().fetchAttemptTasks(this.props.attemptId, {
      success: tasks => {
        if (!this.ignoreLastFetch) {
          this.setState({tasks});
        }
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
  ignoreLastFetch: boolean;

  props: {
    file: LogFile;
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

  componentDidUpdate(prevProps: {file: LogFile}) {
    const oldFileName = prevProps.file.fileName;
    const newFileName = this.props.file.fileName;
    if (newFileName !== oldFileName) {
      this.fetchFile()
    }
  }

  fetchFile() {
    model().fetchLogFile(this.props.file, {
      success: data => {
        if (!this.ignoreLastFetch) {
          this.setState({data});
        }
      }
    });
  }

  render() {
    return this.state.data
      ? <pre>{pako.inflate(this.state.data, {to: 'string'})}</pre>
      : <pre></pre>;
  }
}


class AttemptLogsView extends React.Component {
  ignoreLastFetch: boolean;

  props: {
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
    const oldId = prevProps.attemptId;
    const newId = this.props.attemptId;
    if (newId !== oldId) {
      this.fetchLogs()
    }
  }

  fetchLogs() {
    model().fetchAttemptLogFileHandles(this.props.attemptId, {
      success: files => {
        if (!this.ignoreLastFetch) {
          this.setState({files});
        }
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

  render() {
    return (
      <nav className="navbar navbar-inverse navbar-fixed-top">
        <div className="container-fluid">
          <div className="navbar-header">
            <button type="button" className="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar"
                    aria-expanded="false" aria-controls="navbar">
              <span className="sr-only">Toggle navigation</span>
              <span className="icon-bar"></span>
              <span className="icon-bar"></span>
              <span className="icon-bar"></span>
            </button>
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
    )
  }
}

const ProjectsPage = (props: {}) =>
  <div className="container-fluid">
    <Navbar />
    <ProjectsView />
    <SessionsView />
  </div>;

const ProjectPage = (props: {params: {projectId: string}}) =>
  <div className="container-fluid">
    <Navbar />
    <ProjectView projectId={props.params.projectId}/>
  </div>;

class WorkflowPage extends React.Component {
  ignoreLastFetch: boolean;

  props: {
    params: {
      projectId: string;
      workflowName: string;
    }
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
    model().fetchProjectWorkflow(this.props.params.projectId, this.props.params.workflowName, {
      success: workflow => {
        if (!this.ignoreLastFetch) {
          this.setState({workflow});
        }
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
  ignoreLastFetch: boolean;

  props: {
    params: {
      workflowId: string;
    };
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
    model().fetchWorkflow(this.props.params.workflowId, {
      success: workflow => {
        if (!this.ignoreLastFetch) {
          this.setState({workflow});
        }
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

const AttemptPage = (props: {params: {attemptId: string}}) =>
  <div className="container-fluid">
    <Navbar />
    <AttemptView attemptId={parseInt(props.params.attemptId)}/>
    <AttemptTasksView attemptId={parseInt(props.params.attemptId)}/>
    <AttemptLogsView attemptId={parseInt(props.params.attemptId)}/>
  </div>;

class SessionPage extends React.Component {
  ignoreLastFetch: boolean;

  props: {
    params: {
      sessionId: string;
    }
  };

  state = {
    session: null,
    tasks: [],
    attempts: [],
  };

  componentDidMount() {
    this.fetchSession();
  }

  componentWillUnmount() {
    this.ignoreLastFetch = true;
  }

  componentDidUpdate(prevProps: {params: {sessionId: string}}) {
    const oldId = prevProps.params.sessionId;
    const newId = this.props.params.sessionId;
    if (newId !== oldId) {
      this.fetchSession()
    }
  }

  fetchSession() {
    model().fetchSession(this.props.params.sessionId, {
      success: session => {
        if (!this.ignoreLastFetch) {
          this.setState({session});
        }
      }
    });
    model().fetchSessionAttempts(this.props.params.sessionId, {
      success: attempts => {
        if (!this.ignoreLastFetch) {
          this.setState({attempts});
        }
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

  state: Credentials;

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

  valid(credentials: Credentials, key: string, value: string) {
    return (key: string) => {
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
    const credentials: Credentials = {};
    for (let item of DIGDAG_CONFIG.auth.items) {
      const key = item.key;
      const scrub: Scrubber = item.scrub ? item.scrub : (args: {key: string, value: string}) => value;
      const value: string = scrub({key, value: this.state[key]});
      item.validate({key, value, valid: this.valid(credentials, key, value), invalid: this.invalid(credentials, key, key)});
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

  state: { authenticated: bool };

  constructor(props: any) {
    super(props);
    // this.handleCredentialsSubmit = this.handleCredentialsSubmit.bind(this);
    const credentials = window.localStorage.getItem("digdag.credentials");
    if (credentials) {
      setupModel(JSON.parse(credentials));
      this.state = {authenticated: true};
    } else {
      this.state = {authenticated: false};
    }
  }

  handleCredentialsSubmit: (credentials: Credentials) => void = (credentials: Credentials) => {
    window.localStorage.setItem("digdag.credentials", JSON.stringify(credentials));
    setupModel(credentials);
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
