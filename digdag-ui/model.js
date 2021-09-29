// @flow

import pako from 'pako'
import untar from 'js-untar'
import { Buffer } from 'buffer/'
import LRU from 'lru-cache'

export type Credentials = {[key: string]: string};
export type Headers = {[key: string]: string};
export type MethodType = 'GET' | 'POST' | 'DELETE' | 'HEAD' | 'OPTIONS' | 'PUT' | 'PATCH'

export type LogFileHandle = {
  fileName: string;
  fileSize: number;
  taskName: string;
  fileTime: string;
  agentId: string;
  direct: ?string;
}

export type LogFileHandleCollection = {
  files: Array<LogFileHandle>;
}

export type TarEntry = {
  name: string;
  buffer: ArrayBuffer;
}

export type IdName = {
  id: string;
  name: string;
}

export type NameOptionalId = {
  name: string;
  id: ?string;
};

export type UUID = string;

export type Workflow = {
  id: string;
  name: string;
  project: IdName;
  revision: string;
  config: Object;
};

export type WorkflowCollection = {
  workflows: Array<Workflow>;
}

export type Project = {
  id: string;
  name: string;
  revision: string;
  createdAt: string;
  updatedAt: string;
  archiveType: string;
  archiveMd5: string;
}

export type ProjectCollection = {
  projects: Array<Project>;
}

export type Task = {
  id: string;
  fullName: string;
  parentId: ?string;
  config: Object;
  upstreams: Array<string>;
  isGroup: boolean;
  state: string;
  cancelRequested: boolean;
  exportParams: Object;
  storeParams: Object;
  stateParams: Object;
  updatedAt: string;
  retryAt: ?string;
  startedAt: ?string;
  order: ?Number;
};

export type TaskCollection = {
  tasks: Array<Task>;
}

export type Attempt = {
  id: string;
  project: IdName;
  workflow: NameOptionalId;
  sessionId: string;
  sessionUuid: UUID;
  sessionTime: string;
  retryAttemptName: ?string;
  done: boolean;
  success: boolean;
  cancelRequested: boolean;
  params: Object;
  createdAt: string;
  finishedAt: string;
};

export type AttemptCollection = {
  attempts: Array<Attempt>;
}

export type Session = {
  id: string;
  project: IdName;
  workflow: NameOptionalId;
  sessionUuid: UUID;
  sessionTime: string;
  lastAttempt: ?{
    id: string;
    retryAttemptName: ?string;
    done: boolean;
    success: boolean;
    cancelRequested: boolean;
    params: Object;
    createdAt: string;
    finishedAt: string;
  };
};

export type IdAndName = {
  id: string;
  name: string;
}

export type Schedule = {
  id: string;
  project: IdAndName;
  workflow: IdAndName;
  nextRunTime: string;
  nextScheduleTime: string;
  disabledAt: ?string;
}

export type ScheduleCollection = {
  schedules: Array<Schedule>;
}

export type SessionCollection = {
  sessions: Array<Session>;
}

export class ProjectArchive {
  files: Array<TarEntry>;
  fileMap: Map<string, TarEntry>;
  legacy: boolean;

  constructor (files: Array<TarEntry>) {
    this.files = files
    this.fileMap = new Map()
    this.legacy = false
    for (let file of files) {
      // If the archive has a digdag.yml (which is now just a normal file and no longer interpreted as a project definition) we assume that
      // the archive might stem from that legacy era and also contain <workflow>.yml files.
      if (file.name === 'digdag.yml') {
        this.legacy = true
      }
      this.fileMap.set(file.name, file)
    }
  }

  getWorkflow (name: string): ?string {
    var buffer = this.getFileContents(`${name}.dig`)
    // Also look for <workflow>.yml if this archive might be a legacy archive.
    if (this.legacy && !buffer) {
      buffer = this.getFileContents(`${name}.yml`)
    }
    return buffer ? buffer.toString() : null
  }

  getFileContents (name: string): ?Buffer {
    const file = this.fileMap.get(name)
    if (!file) {
      return null
    }
    return new Buffer(file.buffer)
  }

  hasFile (name: string): boolean {
    return this.fileMap.has(name)
  }
}

export type HeadersProvider = (args: {credentials: Credentials}) => Headers;

export type ModelConfig = {
  url: string;
  td: {
    useTD: boolean;
    apiV4: string;
    connectorUrl: (id: string) => string;
    queryUrl: (id: string) => string;
    jobUrl: (id: string) => string;
  };
  credentials: Credentials;
  headers: HeadersProvider;
}

export class Model {
  config: ModelConfig;
  workflowCache: LRU;
  queriesCache: LRU;

  constructor (config: ModelConfig) {
    this.config = config
    this.workflowCache = LRU({ max: 10000 })
    this.queriesCache = LRU({ max: 10000 })
  }

  fetchProjects (): Promise<ProjectCollection> {
    return this.get(`projects`)
  }

  fetchWorkflows (): Promise<WorkflowCollection> {
    return this.get(`workflows`)
  }

  fetchProject (projectId: string): Promise<Project> {
    return this.get(`projects/${projectId}`)
  }

  fetchWorkflow (workflowId: string): Promise<Workflow> {
    const id = workflowId.toString()
    let workflow = this.workflowCache.get(id)
    if (workflow) {
      return workflow
    }
    workflow = this.get(`workflows/${id}`)
    this.workflowCache.set(id, workflow)
    const model = this
    workflow.catch((error) => {
      model.workflowCache.delete(id)
      throw error
    })
    return workflow
  }

  fetchProjectWorkflows (projectId: string): Promise<WorkflowCollection> {
    return this.get(`projects/${projectId}/workflows`)
  }

  fetchProjectWorkflow (projectId: string, workflowName: string): Promise<Workflow> {
    return this.get(`projects/${projectId}/workflows/${encodeURIComponent(workflowName)}`)
  }

  fetchProjectWorkflowAttempts (projectName: string, workflowName: string): Promise<AttemptCollection> {
    return this.get(`attempts?project=${encodeURIComponent(projectName)}&workflow=${encodeURIComponent(workflowName)}`)
  }

  fetchProjectWorkflowSessions (projectId: string, workflowName: string): Promise<SessionCollection> {
    return this.get(`projects/${projectId}/sessions?workflow=${encodeURIComponent(workflowName)}`)
  }

  fetchProjectSessions (projectId: string): Promise<SessionCollection> {
    return this.get(`projects/${projectId}/sessions`)
  }

  fetchProjectAttempts (projectName: string): Promise<AttemptCollection> {
    return this.get(`attempts?project=${encodeURIComponent(projectName)}`)
  }

  fetchAttempts (): Promise<AttemptCollection> {
    return this.get(`attempts`)
  }

  fetchSessions (pageSize: number, lastId: ?number): Promise<SessionCollection> {
    if (lastId) {
      return this.get(`sessions?page_size=${pageSize}&last_id=${lastId}`)
    }
    return this.get(`sessions?page_size=${pageSize}`)
  }

  fetchAttempt (attemptId: string): Promise<Attempt> {
    return this.get(`attempts/${attemptId}`)
  }

  fetchSession (sessionId: string): Promise<Session> {
    return this.get(`sessions/${sessionId}`)
  }

  fetchSessionAttempts (sessionId: string): Promise<AttemptCollection> {
    return this.get(`sessions/${sessionId}/attempts?include_retried=true`)
  }

  fetchAttemptTasks (attemptId: string): Promise<Map<string, Task>> {
    return this.get(`attempts/${attemptId}/tasks`)
      .then(taskCollection => new Map(taskCollection.tasks.map(task => [task.id, task])))
  }

  fetchAttemptLogFileHandles (attemptId: string): Promise<LogFileHandleCollection> {
    return this.get(`logs/${attemptId}/files`)
  }

  fetchAttemptTaskLogFileHandles (attemptId: string, taskName: string): Promise<LogFileHandleCollection> {
    return this.get(`logs/${attemptId}/files?task=${encodeURIComponent(taskName)}`)
  }

  fetchLogFile (attemptId: string, file: LogFileHandle) {
    if (file.direct) {
      return this.fetchArrayBuffer(file.direct, true)
    } else {
      return this.fetchArrayBuffer(`${this.config.url}logs/${attemptId}/files/${encodeURIComponent(file.fileName)}`, false)
    }
  }

  fetchProjectArchiveLatest (projectId: string): Promise<ProjectArchive> {
    return fetch(this.config.url + `projects/${projectId}/archive`, {
      credentials: 'include',
      headers: this.headers()
    }).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      return response.arrayBuffer().then(data => {
        return untar(pako.inflate(data)).then(files => {
          return new ProjectArchive((files))
        })
      })
    })
  }

  fetchProjectArchiveWithRevision (projectId: string, revisionName: string): Promise<ProjectArchive> {
    return fetch(this.config.url + `projects/${projectId}/archive?revision=${encodeURIComponent(revisionName)}`, {
      credentials: 'include',
      headers: this.headers()
    }).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      return response.arrayBuffer().then(data => {
        return untar(pako.inflate(data).buffer)
      }).then(files => {
        return new ProjectArchive(files)
      })
    })
  }

  putProject (projectName: string, revision: string, targz: ArrayBuffer): Promise<Project> {
    return this.putBinary(`projects?project=${projectName}&revision=${revision}`, 'application/gzip', targz)
  }

  retrySession (session: Session, sessionUUID: string) {
    const { lastAttempt } = session
    return this.put('attempts', {
      workflowId: session.workflow.id,
      params: lastAttempt && lastAttempt.params,
      sessionTime: session.sessionTime,
      retryAttemptName: sessionUUID
    })
  }

  startAttempt (workflowId: string, sessionTime: string, params: Object) {
    return this.put('attempts', { workflowId, sessionTime, params })
  }

  killAttempt (attemptId: string) {
    return this.post(`attempts/${attemptId}/kill`)
  }

  retrySessionWithLatestRevision (session: Session, attemptName: string) {
    const { lastAttempt, project, workflow } = session
    const model = this
    return this.fetchProjectWorkflow(project.id, workflow.name).then((result) =>
      model.put('attempts', {
        workflowId: result.id,
        params: lastAttempt && lastAttempt.params,
        sessionTime: session.sessionTime,
        retryAttemptName: attemptName
      })
    )
  }

  resumeSessionWithLatestRevision (session: Session, attemptName: string, attemptId: string) {
    const { lastAttempt, project, workflow } = session
    const model = this
    return this.fetchProjectWorkflow(project.id, workflow.name).then((result) =>
      model.put('attempts', {
        workflowId: result.id,
        params: lastAttempt && lastAttempt.params,
        sessionTime: session.sessionTime,
        retryAttemptName: attemptName,
        resume: {
          mode: 'failed',
          attemptId: attemptId
        }
      })
    )
  }

  fetchProjectWorkflowSchedule (projectId: string, workflowName: string): Promise<ScheduleCollection> {
    return this.get(`projects/${projectId}/schedules?workflow=${workflowName}`)
  }

  fetchVersion () {
    return this.get('version')
  }

  enableSchedule (scheduleId: string) : Promise<*> {
    return this.post(`schedules/${scheduleId}/enable`)
  }

  disableSchedule (scheduleId: string) : Promise<*> {
    return this.post(`schedules/${scheduleId}/disable`)
  }

  getTDQueryIdFromName (queryName: string) : string {
    const query = this.queriesCache.get(queryName, null)
    if (!query) {
      return ''
    }
    return query.id
  }

  fillTDQueryCache () : Promise<*> {
    const model = this
    if (!this.config.td.useTD) {
      return Promise.resolve({})
    }
    return fetch(this.config.td.apiV4 + '/queries', {
      credentials: 'include',
      headers: this.headers()
    }).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      return response.json()
    }).then((queries) => {
      queries.forEach((query) => {
        model.queriesCache.set(query.name, query)
      })
    })
  }

  get (url: string): Promise<*> {
    return this.http(url, 'GET')
  }

  post (url: string): Promise<*> {
    return this.http(url, 'POST')
  }

  http (url: string, method: MethodType): Promise<*> {
    return fetch(this.config.url + url, {
      credentials: 'include',
      method,
      headers: Object.assign({}, this.headers(), {
        'Content-Type': 'application/json',
      }),
    }).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      if (response.status === 204) {
        return null
      }
      return response.json()
    })
  }

  put (url: string, body: any): Promise<*> {
    return fetch(this.config.url + url, {
      credentials: 'include',
      headers: Object.assign({}, this.headers(), {
        'Content-Type': 'application/json'
      }),
      method: 'PUT',
      body: JSON.stringify(body)
    }).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      return response.json()
    })
  }

  putBinary (url: string, contentType: string, body: ArrayBuffer): Promise<*> {
    return fetch(this.config.url + url, {
      credentials: 'include',
      headers: Object.assign({}, this.headers(), {
        'Content-Type': contentType,
        'Content-Length': body.byteLength.toString()
      }),
      method: 'PUT',
      body: new global.Blob([body], { type: contentType })
    }).then(response => {
      if (!response.ok) {
        return response.text().then(text => {
          throw new Error(`${text} (${response.statusText})`)
        })
      }
      return response.json()
    })
  }

  fetchArrayBuffer (url: string, directUrl: boolean) {
    let options = {}
    if (!directUrl) {
      // if the URL is the direct url given by the server, client shouldn't send credentials to the host
      // because the host is not always trusted and might not have exact Access-Control-Allow-Origin
      // (browser rejects "Access-Control-Allow-Origin: '*'" if "credentials: include" is set).
      // Instead, the URL itself should include enough information to authenticate the client such as a
      // pre-signed temporary token as a part of query string.
      options['credentials'] = 'include'
    }
    return fetch(url, options).then(response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      return response.arrayBuffer()
    })
  }

  headers (): Headers {
    return this.config.headers({ credentials: this.config.credentials })
  }
}

var instance: ?Model = null

export function setup (config: ModelConfig) {
  instance = new Model(config)
}

export function model (): Model {
  if (!instance) {
    throw new Error('Model not yet setup')
  }
  return instance
}
