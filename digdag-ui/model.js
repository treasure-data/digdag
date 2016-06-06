// @flow

import pako from 'pako';
import untar from 'js-untar';
import {Buffer} from 'buffer/';
import LRU from 'lru-cache';

export type Credentials = {[key: string]: string};
export type Headers = {[key: string]: string};

export type DirectDownloadHandle = {
  type: string;
  url: string;
}

export type LogFileHandle = {
  fileName: string;
  fileSize: number;
  taskName: string;
  fileTime: string;
  agentId: string;
  direct: ?DirectDownloadHandle;
}

export type TarEntry = {
  name: string;
  buffer: ArrayBuffer;
}

export type IdName = {
  id: number;
  name: string;
}

export type NameOptionalId = {
  name: string;
  id: ?number;
};

export type UUID = string;

export type Workflow = {
  id: number;
  name: string;
  project: IdName;
  revision: string;
  config: Object;
};

export type Project = {
  id: number;
  name: string;
  revision: string;
  createdAt: string;
  updatedAt: string;
  archiveType: string;
  archiveMd5: string;
}

export type Task = {
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

export type Attempt = {
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

export type Session = {
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

export class ProjectArchive {
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

  getWorkflow(name: string): ?string {
    const suffix = this.legacy ? 'yml' : 'dig';
    const filename = `${name}.${suffix}`;
    const buffer = this.getFileContents(filename);
    return buffer ? buffer.toString() : null;
  }

  getFileContents(name: string): ?Buffer {
    const file = this.fileMap.get(name);
    if (!file) {
      return null;
    }
    return new Buffer(file.buffer);
  }

  hasFile(name: string): boolean {
    return this.fileMap.has(name);
  }
}

export type HeadersProvider = (args: {credentials: Credentials}) => Headers;

export type ModelConfig = {
  url: string;
  credentials: Credentials;
  headers: HeadersProvider;
}

export class Model {
  config: ModelConfig;
  workflowCache: LRU;

  constructor(config: ModelConfig) {
    this.config = config;
    this.workflowCache = LRU({ max: 10000 });
  }

  fetchProjects(): Promise<Array<Project>> {
    return this.get(`projects/`);
  }

  fetchProject(projectId: number): Promise<Project> {
    return this.get(`projects/${projectId}`);
  }

  fetchWorkflow(workflowId: number): Promise<Workflow> {
    const id = workflowId.toString();
    let workflow = this.workflowCache.get(id);
    if (workflow) {
      return workflow;
    }
    workflow = this.get(`workflows/${id}`);
    this.workflowCache.set(id, workflow);
    workflow.catch(error => {
      this.workflowCache.delete(id);
    });
    return workflow;
  }

  fetchProjectWorkflows(projectId: number): Promise<Array<Workflow>> {
    return this.get(`projects/${projectId}/workflows`);
  }

  fetchProjectWorkflow(projectId: number, workflowName: string): Promise<Workflow> {
    return this.get(`projects/${projectId}/workflows/${encodeURIComponent(workflowName)}`);
  }

  fetchProjectWorkflowAttempts(projectName: string, workflowName: string): Promise<Array<Attempt>> {
    return this.get(`attempts?project=${encodeURIComponent(projectName)}&workflow=${encodeURIComponent(workflowName)}`);
  }

  fetchProjectWorkflowSessions(projectId: number, workflowName: string): Promise<Array<Session>> {
    return this.get(`projects/${projectId}/sessions?workflow=${encodeURIComponent(workflowName)}`);
  }

  fetchProjectSessions(projectId: number): Promise<Array<Session>> {
    return this.get(`projects/${projectId}/sessions`);
  }

  fetchProjectAttempts(projectName: string): Promise<Array<Attempt>> {
    return this.get(`attempts?project=${encodeURIComponent(projectName)}`);
  }

  fetchAttempts(): Promise<Array<Attempt>> {
    return this.get(`attempts`);
  }

  fetchSessions(): Promise<Array<Session>> {
    return this.get(`sessions`);
  }

  fetchAttempt(attemptId: number): Promise<Attempt> {
    return this.get(`attempts/${attemptId}`);
  }

  fetchSession(sessionId: number): Promise<Session> {
    return this.get(`sessions/${sessionId}`);
  }

  fetchSessionAttempts(sessionId: number): Promise<Array<Attempt>> {
    return this.get(`sessions/${sessionId}/attempts?include_retried=true`);
  }

  fetchAttemptTasks(attemptId: number): Promise<Array<Task>> {
    return this.get(`attempts/${attemptId}/tasks`);
  }

  fetchAttemptLogFileHandles(attemptId: number): Promise<Array<LogFileHandle>> {
    return this.get(`logs/${attemptId}/files`);
  }

  fetchAttemptTaskLogFileHandles(attemptId: number, taskName: string): Promise<Array<LogFileHandle>> {
    return this.get(`logs/${attemptId}/files?task=${encodeURIComponent(taskName)}`);
  }

  fetchLogFile(file: LogFileHandle) {
    if (!file.direct) {
      return new Promise((resolve, reject) => {
        reject(`Cannot fetch non-direct log file: ${file.fileName}`);
      });
    }
    return fetch(file.direct.url).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText);
      }
      return response.arrayBuffer();
    });
  }

  fetchProjectArchiveLatest(projectId: number): Promise<ProjectArchive> {
    return fetch(this.config.url + `projects/${projectId}/archive`, {
      headers: this.headers(),
    }).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText);
      }
      return response.arrayBuffer().then(data => {
        return untar(pako.inflate(data)).then(files => {
          return new ProjectArchive((files: Array<TarEntry>));
        });
      });
    });
  }

  fetchProjectArchiveWithRevision(projectId: number, revisionName: string): Promise<ProjectArchive> {
    return fetch(this.config.url + `projects/${projectId}/archive?revision=${encodeURIComponent(revisionName)}`, {
      headers: this.headers(),
    }).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText);
      }
      return response.arrayBuffer().then(data => {
        return untar(pako.inflate(data).buffer);
      }).then(files => {
        return new ProjectArchive(files);
      });
    });
  }


  get(url: string): Promise {
    return fetch(this.config.url + url, {
      headers: this.headers(),
    }).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText);
      }
      return response.json();
    });
  }

  headers(): Headers {
    return this.config.headers({credentials: this.config.credentials});
  }
}

var instance: ?Model = null;

export function setup(config: ModelConfig) {
  instance = new Model(config);
}

export function model(): Model {
  if (!instance) {
    throw new Error("Model not yet setup");
  }
  return instance;
}

