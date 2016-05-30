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

function ab2str(buf) {
  return new TextDecoder().decode(buf);
}

class ProjectArchive {
  constructor(files) {
    this.files = files;
    this.fileMap = new Map();
    for (let file of files) {
      if (file.name == 'digdag.yml') {
        this.definition = jsyaml.safeLoad(ab2str(file.buffer));
      }
      this.fileMap.set(file.name, file);
    }
  }

  getWorkflow(name) {
    const filename = `${name}.yml`;
    const file = this.fileMap.get(filename);
    if (!file) {
      return null;
    }
    return ab2str(file.buffer);
  }
}

class Model {
  constructor(url, credentials) {
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

  fetchProjectWorkflowAttempts(projectName, workflowName, callbacks) {
    this.get(`attempts?project=${encodeURIComponent(projectName)}&workflow=${encodeURIComponent(workflowName)}`, callbacks);
  }

  fetchProjectAttempts(projectName, callbacks) {
    this.get(`attempts?project=${encodeURIComponent(projectName)}`, callbacks);
  }

  fetchAttempts(callbacks) {
    this.get(`attempts`, callbacks);
  }

  fetchAttempt(attemptId, callbacks) {
    this.get(`attempts/${attemptId}`, callbacks);
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
        untar(pako.inflate(tgz)).then(files => {
          const archive = new ProjectArchive(files);
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

class ProjectList extends React.Component {
  render() {
    const projectRows = this.props.projects.map(project =>
      <tr key={project.id}>
        <td><Link to={`/projects/${project.id}`}>{project.id}</Link></td>
        <td><Link to={`/projects/${project.id}`}>{project.name}</Link></td>
        <td>{project.revision}</td>
        <td>{formatTimestamp(project.updatedAt)}</td>
      </tr>
    );
    return (
      <div className="table-responsive">
        <table className="table table-striped table-hover table-condensed">
          <thead>
          <tr>
            <th>ID</th>
            <th>Name</th>
            <th>Revision</th>
            <th>Updated</th>
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

class WorkflowList extends React.Component {
  render() {
    const rows = this.props.workflows.map(workflow =>
      <tr key={workflow.id}>
        <td><Link to={`/workflows/${workflow.id}`}>{workflow.id}</Link></td>
        <td><Link to={`/workflows/${workflow.id}`}>{workflow.name}</Link></td>
      </tr>
    );
    return (
      <div className="table-responsive">
        <table className="table table-striped table-hover table-condensed">
          <thead>
          <tr>
            <th>ID</th>
            <th>Name</th>
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

class AttemptList extends React.Component {

  projectData(attempt) {
    if (this.props.showProject) {
      return <td><Link to={`/projects/${attempt.project.id}`}>{attempt.project.name}</Link></td>;
    } else {
      return null;
    }
  }

  projectHead() {
    if (this.props.showProject) {
      return <th>Project</th>;
    } else {
      return null;
    }
  }

  render() {
    const rows = this.props.attempts.map(attempt => {
      return (
        <tr key={attempt.id}>
          <td><Link to={`/attempts/${attempt.id}`}>{attempt.id}</Link></td>
          {this.projectData(attempt)}
          <td><Link to={`/workflows/${attempt.workflow.id}`}>{attempt.workflow.name}</Link></td>
          <td>{formatTimestamp(attempt.createdAt)}</td>
          <td>{formatSessionTime(attempt.sessionTime)}</td>
          <td>{attemptStatus(attempt)}</td>
        </tr>
      );
    });

    return (
      <div className="table-responsive">
        <table className="table table-striped table-hover table-condensed">
          <thead>
          <tr>
            <th>ID</th>
            {this.projectHead()}
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
    );
  }
}

class Projects extends React.Component {

  state = {
    projects: [],
  };

  componentDidMount() {
    model().fetchProjects({
      success: projects => {
        this.setState({projects: projects});
      }
    });
  }

  render() {
    return (
      <div className="projects">
        <h2>Projects</h2>
        <ProjectList projects={this.state.projects}/>
      </div>
    );
  }
}

class Attempts extends React.Component {

  state = {
    attempts: [],
  };

  componentDidMount() {
    model().fetchAttempts({
      success: attempts => {
        this.setState({attempts: attempts});
      }
    });
  }

  render() {
    return (
      <div>
        <h2>Attempts</h2>
        <AttemptList attempts={this.state.attempts} showProject={true}/>
      </div>
    );
  }
}

class Project extends React.Component {

  state = {
    project: {},
    workflows: [],
    attempts: [],
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
          // TODO: make this fetchable by project ID
          model().fetchProjectAttempts(this.state.project.name, {
            success: attempts => {
              if (!this.ignoreLastFetch) {
                this.setState({attempts: attempts});
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
              <td>{formatTimestamp(project.createdAt)}</td>
            </tr>
            <tr>
              <td>Updated</td>
              <td>{formatTimestamp(project.updatedAt)}</td>
            </tr>
            </tbody>
          </table>
        </div>
        <div className="row">
          <h2>Workflows</h2>
          <WorkflowList workflows={this.state.workflows}/>
        </div>
        <div className="row">
          <h2>Attempts</h2>
          <AttemptList attempts={this.state.attempts}/>
        </div>
      </div>
    );

  }
}

class Workflow extends React.Component {

  state = {
    workflow: null,
    attempts: [],
    projectArchive: null,
  };

  componentDidMount() {
    this.fetchWorkflow();
  }

  componentWillUnmount() {
    this.ignoreLastFetch = true;
  }

  componentDidUpdate(prevProps) {
    const oldId = prevProps.workflowId;
    const newId = this.props.workflowId;
    if (newId !== oldId) {
      this.fetchWorkflow()
    }
  }

  fetchWorkflow() {
    model().fetchWorkflow(this.props.workflowId, {
      success: workflow => {
        if (!this.ignoreLastFetch) {
          this.setState({workflow: workflow});
          model().fetchProjectWorkflowAttempts(this.state.workflow.project.name, this.state.workflow.name, {
            success: attempts => {
              if (!this.ignoreLastFetch) {
                this.setState({attempts});
              }
            }
          });
          model().fetchProjectArchiveWithRevision(this.state.workflow.project.id, this.state.workflow.revision, {
            success: projectArchive => {
              if (!this.ignoreLastFetch) {
                this.setState({projectArchive});
              }
            }
          });
        }
      }
    });
  }

  definition() {
    if (!this.state.projectArchive || !this.state.workflow) {
      return '';
    }
    const workflow = this.state.projectArchive.getWorkflow(this.state.workflow.name);
    if (!workflow) {
      return '';
    }
    return workflow.trim();
  }

  render() {
    const workflow = this.state.workflow;

    if (!workflow) {
      return null;
    }

    return (
      <div>
        <div className="row">
          <h2>Workflow</h2>
          <table className="table table-condensed">
            <tbody>
            <tr>
              <td>ID</td>
              <td>{workflow.id}</td>
            </tr>
            <tr>
              <td>Name</td>
              <td>{workflow.name}</td>
            </tr>
            <tr>
              <td>Project</td>
              <td><Link to={`/projects/${workflow.project.id}`}>{workflow.project.name}</Link></td>
            </tr>
            <tr>
              <td>Revision</td>
              <td>{workflow.revision}</td>
            </tr>
            </tbody>
          </table>
        </div>
        <div className="row">
          <h2>Definition</h2>
          <pre><PrismCode className="language-yaml">{this.definition()}</PrismCode></pre>
        </div>
        <div className="row">
          <h2>Attempts</h2>
          <AttemptList attempts={this.state.attempts} showProject={true}/>
        </div>
      </div>
    );
  }
}

class Attempt extends React.Component {

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

function formatSessionTime(dt) {
  if (!dt) {
    return '';
  }
  const m = moment(dt);
  return <span>{dt} ({m.fromNow()})</span>;
}

function formatTimestamp(ts) {
  if (!ts) {
    return '';
  }
  const m = moment(ts);
  return <span>{ts} ({m.fromNow()})</span>;
}

class TaskList extends React.Component {
  render() {
    const rows = this.props.tasks.map(task => {
      return (
        <tr key={task.id}>
          <td>{task.id}</td>
          <td>{task.fullName}</td>
          <td>{task.parentId}</td>
          <td>{formatSessionTime(task.sessionTime)}</td>
          <td>{formatTimestamp(task.updatedAt)}</td>
          <td>{task.state}</td>
          <td>{formatTimestamp(task.retryAt)}</td>
        </tr>
      );
    });

    return (
      <div className="table-responsive">
        <table className="table table-striped table-hover table-condensed">
          <thead>
          <tr>
            <th>ID</th>
            <th>Name</th>
            <th>Parent ID</th>
            <th>Session Time</th>
            <th>Updated</th>
            <th>State</th>
            <th>Retry</th>
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


class AttemptTasks extends React.Component {

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
    const tasks = this.state.tasks;
    return (
      <div className="row">
        <h2>Tasks</h2>
        <TaskList tasks={this.state.tasks}/>
      </div>
    );
  }
}

class LogFile extends React.Component {

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
    if (!this.state.data) {
      return null;
    }
    return <pre>{pako.inflate(this.state.data, {to: 'string'})}</pre>;
  }
}


class AttemptLogs extends React.Component {

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
    return this.state.files.map(file => {
      return <LogFile key={file.fileName} file={file}/>;
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

class Version extends React.Component {
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
            <p className="navbar-text navbar-right"><Version /></p>
          </div>
        </div>
      </nav>
    )
  }
}

class ProjectsPage extends React.Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div className="container-fluid">
        <Navbar />
        <Projects />
        <Attempts />
      </div>
    );
  }
}

class ProjectPage extends React.Component {
  render() {
    return (
      <div className="container-fluid">
        <Navbar />
        <Project projectId={this.props.params.projectId}/>
      </div>
    );
  }
}

class WorkflowPage extends React.Component {
  render() {
    return (
      <div className="container-fluid">
        <Navbar />
        <Workflow workflowId={this.props.params.workflowId}/>
      </div>
    );
  }
}

class AttemptPage extends React.Component {
  render() {
    return (
      <div className="container-fluid">
        <Navbar />
        <Attempt attemptId={this.props.params.attemptId}/>
        <AttemptTasks attemptId={this.props.params.attemptId}/>
        <AttemptLogs attemptId={this.props.params.attemptId}/>
      </div>
    );
  }
}

class LoginPage extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
    DIGDAG_CONFIG.auth.items.forEach(item => {
      this.state[item.key] = '';
    });
    this.onChange = this.onChange.bind(this);
    this.handleSubmit = this.handleSubmit.bind(this)
  }

  onChange(key) {
    return e => {
      e.preventDefault();
      const state = {};
      state[key] = e.target.value;
      this.setState(state);
    };
  }

  valid(values, key, value) {
    return (key) => {
      values[key] = value;
      if (DIGDAG_CONFIG.auth.items.length == Object.keys(values).length) {
        this.props.onSubmit(values);
      }
    }
  }

  invalid(values, key, value, message) {
    return (key) => {
      console.log(`${key} is invalid: message=${message})`);
    };
  }

  handleSubmit(e) {
    e.preventDefault();
    const values = {};
    for (let item of DIGDAG_CONFIG.auth.items) {
      const key = item.key;
      const scrub = item.scrub ? item.scrub : (value) => value;
      const value = scrub({key, value: this.state[key]});
      item.validate({key, value, valid: this.valid(values, key, value), invalid: this.invalid(values, key, key)});
    }
  }

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
          <Route path="/workflows/:workflowId" component={WorkflowPage}/>
          <Route path="/attempts/:attemptId" component={AttemptPage}/>
        </Router>
      </div>
    );
  }
}

export default class Console extends React.Component {
  state = {
    authenticated: false,
  };

  constructor(props) {
    super(props);
    this.handleCredentialsSubmit = this.handleCredentialsSubmit.bind(this);
    const credentials = window.localStorage.getItem("digdag.credentials");
    if (credentials) {
      setupModel(JSON.parse(credentials));
      this.state.authenticated = true;
    }
  }

  handleCredentialsSubmit(credentials) {
    window.localStorage.setItem("digdag.credentials", JSON.stringify(credentials));
    setupModel(credentials);
    this.setState({authenticated: true});
  }

  render() {
    if (this.state.authenticated) {
      return <ConsolePage />;
    } else {
      return <LoginPage onSubmit={this.handleCredentialsSubmit}/>;
    }
  }
}
