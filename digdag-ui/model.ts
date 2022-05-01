import pako from 'pako'
import untar from 'js-untar'
import { Buffer } from 'buffer/'
import LRU from 'lru-cache'

export interface Credentials {[key: string]: string}
export interface Headers {[key: string]: string}
export type MethodType = 'GET' | 'POST' | 'DELETE' | 'HEAD' | 'OPTIONS' | 'PUT' | 'PATCH'

export interface LogFileHandle {
  fileName: string
  fileSize: number
  taskName: string
  fileTime: string
  agentId: string
  direct?: string
}

export interface LogFileHandleCollection {
  files: LogFileHandle[]
}

export interface TarEntry {
  name: string
  buffer: ArrayBuffer
}

export interface IdName {
  id: string
  name: string
}

export interface NameOptionalId {
  name: string
  id?: string
}

export type UUID = string

export interface Workflow {
  id: string
  name: string
  project: IdName
  revision: string
  config: Object
}

export interface WorkflowCollection {
  workflows: Workflow[]
}

export interface Project {
  id: string
  name: string
  revision: string
  createdAt: string
  updatedAt: string
  archiveType: string
  archiveMd5: string
}

export interface ProjectCollection {
  projects: Project[]
}

export interface Task {
  id: string
  fullName: string
  parentId?: string
  config: Object
  upstreams: string[]
  isGroup: boolean
  state: string
  cancelRequested: boolean
  exportParams: Object
  storeParams: Object
  stateParams: Object
  updatedAt: string
  retryAt?: string
  startedAt?: string
  order?: number
}

export interface TaskCollection {
  tasks: Task[]
}

export interface Attempt {
  id: string
  project: IdName
  workflow: NameOptionalId
  sessionId: string
  sessionUuid: UUID
  sessionTime: string
  retryAttemptName?: string
  done: boolean
  success: boolean
  cancelRequested: boolean
  params: Object
  createdAt: string
  finishedAt: string
}

export interface AttemptCollection {
  attempts: Attempt[]
}

export interface Session {
  id: string
  project: IdName
  workflow: NameOptionalId
  sessionUuid: UUID
  sessionTime: string
  lastAttempt?: {
    id: string
    retryAttemptName?: string
    done: boolean
    success: boolean
    cancelRequested: boolean
    params: Object
    createdAt: string
    finishedAt: string
  }
}

export interface IdAndName {
  id: string
  name: string
}

export interface Schedule {
  id: string
  project: IdAndName
  workflow: IdAndName
  nextRunTime: string
  nextScheduleTime: string
  revision: string
  disabledAt?: string
}

export interface ScheduleCollection {
  schedules: Schedule[]
}

export interface SessionCollection {
  sessions: Session[]
}

export class ProjectArchive {
  files: TarEntry[]
  fileMap: Map<string, TarEntry>
  legacy: boolean

  constructor (files: TarEntry[]) {
    this.files = files
    this.fileMap = new Map()
    this.legacy = false
    for (const file of files) {
      // If the archive has a digdag.yml (which is now just a normal file and no longer interpreted as a project definition) we assume that
      // the archive might stem from that legacy era and also contain <workflow>.yml files.
      if (file.name === 'digdag.yml') {
        this.legacy = true
      }
      this.fileMap.set(file.name, file)
    }
  }

  getWorkflow (name: string): string | null {
    let buffer = this.getFileContents(`${name}.dig`)
    // Also look for <workflow>.yml if this archive might be a legacy archive.
    if (this.legacy && (buffer == null)) {
      buffer = this.getFileContents(`${name}.yml`)
    }
    return (buffer != null) ? buffer.toString() : null
  }

  getFileContents (name: string): Buffer | null {
    const file = this.fileMap.get(name)
    if (file == null) {
      return null
    }
    return new Buffer(file.buffer)
  }

  hasFile (name: string): boolean {
    return this.fileMap.has(name)
  }
}

export type HeadersProvider = (args: {credentials: Credentials}) => Headers

export interface ModelConfig {
  url: string
  td: {
    useTD: boolean
    apiV4: string
    connectorUrl: (id: string) => string
    queryUrl: (id: string) => string
    jobUrl: (id: string) => string
  }
  credentials: Credentials
  headers: HeadersProvider
}

export class Model {
  config: ModelConfig
  workflowCache: LRU.Cache<string, Promise<Workflow>>
  queriesCache: LRU.Cache<string, any>

  constructor (config: ModelConfig) {
    this.config = config
    this.workflowCache = (LRU as any)({ max: 10000 }) // need help: why keyword `new` isn't here?
    this.queriesCache = (LRU as any)({ max: 10000 })
  }

  async fetchProjects (): Promise<ProjectCollection> {
    return await this.get('projects')
  }

  async fetchWorkflows (): Promise<WorkflowCollection> {
    return await this.get('workflows')
  }

  async fetchProject (projectId: string): Promise<Project> {
    return await this.get(`projects/${projectId}`)
  }

  async fetchWorkflow (workflowId: string): Promise<Workflow> {
    const id = workflowId.toString()
    let workflow = this.workflowCache.get(id)
    if (workflow != null) {
      return await workflow
    }
    workflow = this.get(`workflows/${id}`)
    this.workflowCache.set(id, workflow)
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const model = this
    workflow.catch((error: unknown) => {
      (model.workflowCache as any).delete(id)
      throw error
    })
    return await workflow
  }

  async fetchProjectWorkflows (projectId: string): Promise<WorkflowCollection> {
    return await this.get(`projects/${projectId}/workflows`)
  }

  async fetchProjectWorkflow (projectId: string, workflowName: string): Promise<Workflow> {
    return await this.get(`projects/${projectId}/workflows/${encodeURIComponent(workflowName)}`)
  }

  async fetchProjectWorkflowAttempts (projectName: string, workflowName: string): Promise<AttemptCollection> {
    return await this.get(`attempts?project=${encodeURIComponent(projectName)}&workflow=${encodeURIComponent(workflowName)}`)
  }

  async fetchProjectWorkflowSessions (projectId: string, workflowName: string): Promise<SessionCollection> {
    return await this.get(`projects/${projectId}/sessions?workflow=${encodeURIComponent(workflowName)}`)
  }

  async fetchProjectSessions (projectId: string): Promise<SessionCollection> {
    return await this.get(`projects/${projectId}/sessions`)
  }

  async fetchProjectAttempts (projectName: string): Promise<AttemptCollection> {
    return await this.get(`attempts?project=${encodeURIComponent(projectName)}`)
  }

  async fetchAttempts (): Promise<AttemptCollection> {
    return await this.get('attempts')
  }

  async fetchSessions (pageSize: number, lastId?: string): Promise<SessionCollection> {
    // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
    if (lastId) {
      return await this.get(`sessions?page_size=${pageSize}&last_id=${lastId}`)
    }
    return await this.get(`sessions?page_size=${pageSize}`)
  }

  async fetchAttempt (attemptId: string): Promise<Attempt> {
    return await this.get(`attempts/${attemptId}`)
  }

  async fetchSession (sessionId: string): Promise<Session> {
    return await this.get(`sessions/${sessionId}`)
  }

  async fetchSessionAttempts (sessionId: string): Promise<AttemptCollection> {
    return await this.get(`sessions/${sessionId}/attempts?include_retried=true`)
  }

  async fetchAttemptTasks (attemptId: string): Promise<Map<string, Task>> {
    return await this.get<TaskCollection>(`attempts/${attemptId}/tasks`)
      .then(taskCollection => new Map(taskCollection.tasks.map(task => [task.id, task])))
  }

  async fetchAttemptLogFileHandles (attemptId: string): Promise<LogFileHandleCollection> {
    return await this.get(`logs/${attemptId}/files`)
  }

  async fetchAttemptTaskLogFileHandles (attemptId: string, taskName: string): Promise<LogFileHandleCollection> {
    return await this.get(`logs/${attemptId}/files?task=${encodeURIComponent(taskName)}`)
  }

  async fetchLogFile (attemptId: string, file: LogFileHandle): Promise<ArrayBuffer> {
    // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
    if (file.direct) {
      return await this.fetchArrayBuffer(file.direct, true)
    } else {
      return await this.fetchArrayBuffer(`${this.config.url}logs/${attemptId}/files/${encodeURIComponent(file.fileName)}`, false)
    }
  }

  async fetchProjectArchiveLatest (projectId: string): Promise<ProjectArchive> {
    return await fetch(this.config.url + `projects/${projectId}/archive`, {
      credentials: 'include',
      headers: this.headers()
    }).then(async response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      return await response.arrayBuffer().then(async data => {
        return await untar(pako.inflate(data as any)).then(files => {
          return new ProjectArchive((files))
        })
      })
    })
  }

  async fetchProjectArchiveWithRevision (projectId: string, revisionName: string): Promise<ProjectArchive> {
    return await fetch(this.config.url + `projects/${projectId}/archive?revision=${encodeURIComponent(revisionName)}`, {
      credentials: 'include',
      headers: this.headers()
    }).then(async response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      return await response.arrayBuffer().then(async data => {
        return await untar(pako.inflate(data as any).buffer)
      }).then(files => {
        return new ProjectArchive(files)
      })
    })
  }

  async putProject (projectName: string, revision: string, targz: ArrayBuffer): Promise<Project> {
    return await this.putBinary(`projects?project=${projectName}&revision=${revision}`, 'application/gzip', targz)
  }

  async retrySession (session: Session, sessionUUID: string): Promise<void> {
    const { lastAttempt } = session
    return await this.put('attempts', {
      workflowId: session.workflow.id,
      // eslint-disable-next-line @typescript-eslint/prefer-optional-chain
      params: (lastAttempt != null) && lastAttempt.params,
      sessionTime: session.sessionTime,
      retryAttemptName: sessionUUID
    })
  }

  async startAttempt (workflowId: string, sessionTime: string, params: Object): Promise<void> {
    return await this.put('attempts', { workflowId, sessionTime, params })
  }

  async killAttempt (attemptId: string): Promise<void> {
    return await this.post(`attempts/${attemptId}/kill`)
  }

  async retrySessionWithLatestRevision (session: Session, attemptName: string): Promise<void> {
    const { lastAttempt, project, workflow } = session
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const model = this
    return await this.fetchProjectWorkflow(project.id, workflow.name).then(async (result) =>
      await model.put('attempts', {
        workflowId: result.id,
        // eslint-disable-next-line @typescript-eslint/prefer-optional-chain
        params: (lastAttempt != null) && lastAttempt.params,
        sessionTime: session.sessionTime,
        retryAttemptName: attemptName
      })
    )
  }

  async resumeSessionWithLatestRevision (session: Session, attemptName: string, attemptId: string): Promise<void> {
    const { lastAttempt, project, workflow } = session
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const model = this
    return await this.fetchProjectWorkflow(project.id, workflow.name).then(async (result) =>
      await model.put('attempts', {
        workflowId: result.id,
        // eslint-disable-next-line @typescript-eslint/prefer-optional-chain
        params: (lastAttempt != null) && lastAttempt.params,
        sessionTime: session.sessionTime,
        retryAttemptName: attemptName,
        resume: {
          mode: 'failed',
          attemptId: attemptId
        }
      })
    )
  }

  async fetchProjectWorkflowSchedule (projectId: string, workflowName: string): Promise<ScheduleCollection> {
    return await this.get(`projects/${projectId}/schedules?workflow=${workflowName}`)
  }

  async fetchVersion (): Promise<string> {
    return await this.get('version')
  }

  async enableSchedule (scheduleId: string): Promise<void> {
    return await this.post(`schedules/${scheduleId}/enable`)
  }

  async disableSchedule (scheduleId: string): Promise<void> {
    return await this.post(`schedules/${scheduleId}/disable`)
  }

  getTDQueryIdFromName (queryName: string): string {
    const query = (this.queriesCache.get as any)(queryName, null)
    // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
    if (!query) {
      return ''
    }
    return query.id
  }

  async fillTDQueryCache (): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const model = this
    if (!this.config.td.useTD) {
      return await Promise.resolve({} as any)
    }
    return await fetch(this.config.td.apiV4 + '/queries', {
      credentials: 'include',
      headers: this.headers()
    }).then(async response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      return await response.json()
    }).then((queries) => {
      queries.forEach((query: any) => {
        model.queriesCache.set(query.name, query)
      })
    })
  }

  async get <T = any>(url: string): Promise<T> {
    return await this.http(url, 'GET')
  }

  async post (url: string): Promise<void> {
    return await this.http(url, 'POST')
  }

  async http (url: string, method: MethodType): Promise<any> {
    return await fetch(this.config.url + url, {
      credentials: 'include',
      method,
      headers: Object.assign({}, this.headers(), {
        'Content-Type': 'application/json'
      })
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

  async put (url: string, body: any): Promise<void> {
    return await fetch(this.config.url + url, {
      credentials: 'include',
      headers: Object.assign({}, this.headers(), {
        'Content-Type': 'application/json'
      }),
      method: 'PUT',
      body: JSON.stringify(body)
    }).then(async response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      return await response.json()
    })
  }

  async putBinary (url: string, contentType: string, body: ArrayBuffer): Promise<Project> {
    return await fetch(this.config.url + url, {
      credentials: 'include',
      headers: Object.assign({}, this.headers(), {
        'Content-Type': contentType,
        'Content-Length': body.byteLength.toString()
      }),
      method: 'PUT',
      body: new (global as any).Blob([body], { type: contentType })
    }).then(async response => {
      if (!response.ok) {
        return await response.text().then(text => {
          throw new Error(`${text} (${response.statusText})`)
        })
      }
      return await response.json()
    })
  }

  async fetchArrayBuffer (url: string, directUrl: boolean): Promise<ArrayBuffer> {
    const options: RequestInit = {}
    if (!directUrl) {
      // if the URL is the direct url given by the server, client shouldn't send credentials to the host
      // because the host is not always trusted and might not have exact Access-Control-Allow-Origin
      // (browser rejects "Access-Control-Allow-Origin: '*'" if "credentials: include" is set).
      // Instead, the URL itself should include enough information to authenticate the client such as a
      // pre-signed temporary token as a part of query string.
      options.credentials = 'include'
    }
    return await fetch(url, options).then(async response => {
      if (!response.ok) {
        throw new Error(response.statusText)
      }
      return await response.arrayBuffer()
    })
  }

  headers (): Headers {
    return this.config.headers({ credentials: this.config.credentials })
  }
}

let instance: Model | null = null

export function setup (config: ModelConfig): void {
  instance = new Model(config)
}

export function model (): Model {
  if (instance == null) {
    throw new Error('Model not yet setup')
  }
  return instance
}
