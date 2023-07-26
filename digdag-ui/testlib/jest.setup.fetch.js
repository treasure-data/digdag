/* eslint-env jest */

// this eslint-disable is intended one. sometimes '${}' appears in workflow definitions.
/* eslint-disable no-template-curly-in-string */

global.fetch = jest.fn((url, { method }) => {
  if (url === '/api/workflows' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({
        workflows: [
          {
            id: '1',
            name: 'error_task',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              _error: { 'py>': 'tasks.error_task.show_error' },
              '+test': { 'py>': 'tasks.error_task.fails' }
            }
          },
          {
            id: '2',
            name: 'generate_subtasks',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+split': {
                'py>': 'tasks.generate_subtasks.ParallelProcess.split'
              },
              '+parallel_process': {
                'py>': 'tasks.generate_subtasks.ParallelProcess.run'
              }
            }
          },
          {
            id: '3',
            name: 'sla_duration',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'America/Los_Angeles',
            config: {
              schedule: { 'hourly>': '15:00' },
              sla: {
                duration: '00:02',
                '+notice': {
                  'sh>':
                    'echo "Workflow session $session_time is not finished yet!"'
                }
              },
              '+sleep': { 'sh>': 'sleep 180' }
            }
          },
          {
            id: '4',
            name: 'js_vars',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+task1': { val: 1, 'sh>': 'echo ${val}' },
              '+task2': { val: '${1 + 1}', 'sh>': 'echo ${val}' },
              '+task3': {
                '+run_command': {
                  val: '${2 + 3}',
                  command: 'echo ${val}',
                  'sh>': '${command}'
                }
              }
            }
          },
          {
            id: '5',
            name: 'basic',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+my_task_1': { 'sh>': 'echo this task runs first.' },
              '+my_task_2': { 'sh>': 'echo this task runs next.' },
              '+any_task_name_here': {
                '+nested_task': {
                  'sh>': 'echo tasks can be nested like this.'
                },
                '+nested_task_2': {
                  'sh>': 'echo nesting is useful for grouping'
                }
              },
              '+parallel_task_foo': {
                _parallel: true,
                '+bar': {
                  'sh>':
                    "echo if 'parallel: true' is set, child tasks run in parallel"
                },
                '+baz': { 'sh>': 'echo bar and baz run in parallel' }
              },
              '+abc': {
                'sh>':
                  'echo please check other examples in examples/ directory for more features.'
              }
            }
          },
          {
            id: '6',
            name: 'schedule_daily',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'America/Los_Angeles',
            config: {
              schedule: { 'daily>': '10:00:00' },
              '+task1': {
                'sh>': 'echo "This schedule is for ${session_time}"'
              }
            }
          },
          {
            id: '7',
            name: 'td_for_each',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+task': {
                'td_for_each>': { data: 'SELECT * FROM nasdaq LIMIT 10' },
                _do: {
                  'echo>':
                    '${td.each.time} ${td.each.symbol} ${td.each.open} ${td.each.volume} ${td.each.high} ${td.each.low} ${td.each.close}'
                },
                database: 'sample_datasets'
              }
            }
          },
          {
            id: '8',
            name: 'emr',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+emr_cluster': {
                'emr>': null,
                cluster: {
                  name: 'my-emr-cluster',
                  ec2: {
                    key: 'my-ec2-key',
                    master_type: 'm3.2xlarge',
                    instances: { type: 'm3.xlarge', count: 3 }
                  },
                  applications: ['spark', 'hive', 'hue'],
                  bootstrap: ['...', '...'],
                  tags: { foo: 'bar' }
                },
                steps: [
                  {
                    type: 'spark',
                    application: 'jars/foobar.jar',
                    args: ['foo', 'the', 'bar'],
                    jars: 'lib/libfoo.jar'
                  },
                  {
                    type: 'spark',
                    application: 'scripts/spark-test.py',
                    args: ['foo', 'the', 'bar']
                  },
                  {
                    type: 'spark-sql',
                    query: 'queries/spark-query.sql',
                    result: 's3://my-bucket/results/${session_uuid}/'
                  },
                  {
                    type: 'hive',
                    script: 'hive/test.q',
                    vars: {
                      INPUT: 's3://my-bucket/hive-input/',
                      OUTPUT: 's3://my-bucket/hive-output/'
                    },
                    hiveconf: { 'hive.support.sql11.reserved.keywords': false }
                  },
                  { type: 'command', command: ['echo', 'hello', 'world'] },
                  { type: 'command', command: ['echo', 'hello', 'world'] }
                ]
              },
              '+emr_steps': {
                'emr>': null,
                cluster: '${emr.last_cluster_id}',
                steps: [
                  {
                    type: 'spark',
                    application: 'jars/foobar.jar',
                    args: ['foo', 'the', 'bar']
                  },
                  {
                    type: 'spark',
                    application: 'scripts/spark-test.py',
                    args: ['foo', 'the', 'bar']
                  },
                  {
                    type: 'spark-sql',
                    query: 'queries/spark-query.sql',
                    result: 's3://my-bucket/results/${session_uuid}/'
                  },
                  {
                    type: 'command',
                    command: 'echo',
                    args: ['hello', 'world']
                  },
                  {
                    type: 'script',
                    command: 'scripts/hello.sh',
                    args: ['foo', 'bar']
                  }
                ]
              }
            }
          },
          {
            id: '9',
            name: 'call',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+step1': { 'sh>': 'echo calling basic...' },
              '+call_another': { 'call>': 'basic' }
            }
          },
          {
            id: '10',
            name: 'python_args',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+a': {
                'py>': 'tasks.python_args.required_arguments',
                required1: '1',
                required2: 2
              },
              '+b': { 'py>': 'tasks.python_args.optional_arguments' },
              '+c': {
                'py>': 'tasks.python_args.mixed_arguments',
                arg1: 'a',
                arg2: { b: 'c' }
              },
              '+d': {
                'py>': 'tasks.python_args.keyword_arguments',
                arg1: 'a',
                key1: 'a',
                key2: 'val2'
              }
            }
          },
          {
            id: '11',
            name: 'export_params',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+set': { 'py>': 'tasks.export_params.set_my_param' },
              '+get': { 'py>': 'tasks.export_params.show_my_param' }
            }
          },
          {
            id: '12',
            name: 'conditions',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+algorithms': {
                '+try': {
                  _parallel: true,
                  '+zlib': { 'py>': 'tasks.conditions.Algorithm.zlib' },
                  '+gzip': { 'py>': 'tasks.conditions.Algorithm.deflate' },
                  '+bzip2': { 'py>': 'tasks.conditions.Algorithm.bzip2' }
                },
                '+decide': {
                  'py>': 'tasks.conditions.Algorithm.decide_algorithm'
                }
              },
              '+show': { 'py>': 'tasks.conditions.show_algorithm' }
            }
          },
          {
            id: '13',
            name: 'treasuredata',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              schedule: { 'daily>': '03:00:00' },
              sla: {
                time: '05:00:00',
                '+mail': {
                  _type: 'mail',
                  body: 'Not finished: ${session_time}',
                  subject: 'Query not finished',
                  to: ['sf@treasure-data.com']
                }
              },
              _error: {
                '+cleanup': { _type: 'td_ddl', delete_table: 'garbage' },
                '+notify': {
                  _type: 'mail',
                  body: 'Query failed!',
                  subject: 'Query not finished',
                  to: ['sf@treasure-data.com']
                }
              },
              '+task': { 'sh>': 'echo NG' }
            }
          },
          {
            id: '14',
            name: 'schedule_weekly',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'America/Los_Angeles',
            config: {
              schedule: { 'weekly>': 'Sun, 10:00:00' },
              '+task1': {
                'sh>': 'echo "This schedule is for ${session_time}"'
              }
            }
          },
          {
            id: '15',
            name: 'schedule_hourly',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'America/Los_Angeles',
            config: {
              schedule: { 'hourly>': '30:00' },
              '+task1': {
                'sh>': 'echo "This schedule is for ${session_time}"'
              }
            }
          },
          {
            id: '16',
            name: 'check_task',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+generate_methods': {
                'py>': 'tasks.check_task.generate',
                _check: { 'py>': 'tasks.check_task.check_generated' }
              },
              '+generate_class_and_params': {
                'py>': 'tasks.check_task.Generator.run',
                _check: { 'py>': 'tasks.check_task.Generator.check' }
              }
            }
          },
          {
            id: '17',
            name: 'ruby_args',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              _export: { rb: { require: 'tasks/ruby_args' } },
              '+task1': { 'rb>': 'Test.task1', arg1: 'this is arg1' },
              '+task2': { 'rb>': 'Test.task2', arg2: 'this is arg2' }
            }
          },
          {
            id: '18',
            name: 'http_for_each',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+fetch': {
                'http>': 'https://jsonplaceholder.typicode.com/users',
                store_content: true
              },
              '+process': {
                'for_each>': { user: '${http.last_content}' },
                _do: { 'echo>': 'Hello ${user.name} @ ${user.company.name}!' }
              }
            }
          },
          {
            id: '19',
            name: 'sla',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'America/Los_Angeles',
            config: {
              schedule: { 'daily>': '02:00:00' },
              sla: {
                time: '02:02',
                '+notice': {
                  'sh>':
                    'echo "Workflow session $session_time is not finished yet!"'
                }
              },
              '+sleep': { 'sh>': 'sleep 180' }
            }
          },
          {
            id: '20',
            name: 'shell_envvar',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+simple': {
                'sh>': './tasks/shell_envvar.sh',
                param1: 'this is param1'
              },
              '+export_to_child_tasks': {
                _export: {
                  exported_var:
                    'this is exported by +export_to_child_tasks task'
                },
                '+child_task_1': { 'sh>': './tasks/shell_envvar.sh' },
                '+child_task_2': { 'sh>': './tasks/shell_envvar.sh' }
              },
              '+simple2': {
                'sh>': './tasks/shell_envvar.sh',
                param2: 'this is param2'
              }
            }
          },
          {
            id: '21',
            name: 'mail',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              _export: {
                mail: {
                  host: 'smtp.gmail.com',
                  port: 587,
                  from: 'you@gmail.com',
                  username: 'you@gmail.com',
                  debug: true
                }
              },
              '+step': {
                'mail>': 'tasks/mail.txt',
                subject: 'this is a smtp test mail',
                to: ['to1@example.com', 'to2@example.com']
              }
            }
          },
          {
            id: '22',
            name: 'include',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+basic': {
                timezone: 'UTC',
                '+my_task_1': { 'sh>': 'echo this task runs first.' },
                '+my_task_2': { 'sh>': 'echo this task runs next.' },
                '+any_task_name_here': {
                  '+nested_task': {
                    'sh>': 'echo tasks can be nested like this.'
                  },
                  '+nested_task_2': {
                    'sh>': 'echo nesting is useful for grouping'
                  }
                },
                '+parallel_task_foo': {
                  _parallel: true,
                  '+bar': {
                    'sh>':
                      "echo if 'parallel: true' is set, child tasks run in parallel"
                  },
                  '+baz': { 'sh>': 'echo bar and baz run in parallel' }
                },
                '+abc': {
                  'sh>':
                    'echo please check other examples in examples/ directory for more features.'
                }
              },
              '+conditions': {
                timezone: 'UTC',
                '+algorithms': {
                  '+try': {
                    _parallel: true,
                    '+zlib': { 'py>': 'tasks.conditions.Algorithm.zlib' },
                    '+gzip': { 'py>': 'tasks.conditions.Algorithm.deflate' },
                    '+bzip2': { 'py>': 'tasks.conditions.Algorithm.bzip2' }
                  },
                  '+decide': {
                    'py>': 'tasks.conditions.Algorithm.decide_algorithm'
                  }
                },
                '+show': { 'py>': 'tasks.conditions.show_algorithm' }
              }
            }
          },
          {
            id: '23',
            name: 'params_child',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+export_params_dependent1': {
                'py>': 'tasks.params.show_export'
              },
              '+export_params_dependent2': {
                'py>': 'tasks.params.show_export'
              }
            }
          },
          {
            id: '24',
            name: 'params',
            project: { id: '1', name: 'example' },
            revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
            timezone: 'UTC',
            config: {
              '+builtin': {
                'sh>': 'echo workflow is running for ${session_time}'
              },
              '+simple': {
                'py>': 'tasks.params.simple',
                data: 'local data',
                number: 'local number'
              },
              '+export_params_1': {
                _export: { mysql: { user: 'exported' } },
                '+step1': { 'py>': 'tasks.params.export_params_step1' },
                '+step2': {
                  'py>': 'tasks.params.export_params_step2',
                  table: 'local table'
                },
                '+export_overwrite': {
                  _export: { mysql: { user: 'overwrite' } },
                  '+nested': { 'py>': 'tasks.params.export_overwrite' }
                },
                '+step3': { 'py>': 'tasks.params.export_params_step3' }
              },
              '+export_params_2': {
                '+set_export_and_call_child': {
                  'py>': 'tasks.params.set_export_and_call_child'
                }
              }
            }
          }
        ]
      })
    })
  }
  if (url === '/api/workflows/5' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({
        id: '5',
        name: 'basic',
        project: { id: '1', name: 'example' },
        revision: 'd232a92c-09dc-4eba-815d-f95d18dc3d7f',
        timezone: 'UTC',
        config: {
          '+my_task_1': { 'sh>': 'echo this task runs first.' },
          '+my_task_2': { 'sh>': 'echo this task runs next.' },
          '+any_task_name_here': {
            '+nested_task': { 'sh>': 'echo tasks can be nested like this.' },
            '+nested_task_2': { 'sh>': 'echo nesting is useful for grouping' }
          },
          '+parallel_task_foo': {
            _parallel: true,
            '+bar': {
              'sh>':
                "echo if 'parallel: true' is set, child tasks run in parallel"
            },
            '+baz': { 'sh>': 'echo bar and baz run in parallel' }
          },
          '+abc': {
            'sh>':
              'echo please check other examples in examples/ directory for more features.'
          }
        }
      })
    })
  }
  if (url === '/api/sessions?page_size=100' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({
        sessions: [
          {
            id: '1',
            project: { id: '1', name: 'example' },
            workflow: { name: 'basic', id: '5' },
            sessionUuid: '1670ab44-c8ca-433e-8f47-ea0c1ddb7f21',
            sessionTime: '2021-09-29T13:17:27+00:00',
            lastAttempt: {
              id: '1',
              retryAttemptName: null,
              done: true,
              success: true,
              cancelRequested: false,
              params: {},
              createdAt: '2021-09-29T13:17:27Z',
              finishedAt: '2021-09-29T13:17:29Z'
            }
          }
        ]
      })
    })
  }
  if (url === '/api/sessions?page_size=1&last_id=1' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({ sessions: [] })
    })
  }
  if (url === '/api/sessions/1' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({
        id: '1',
        project: { id: '1', name: 'example' },
        workflow: { name: 'basic', id: '5' },
        sessionUuid: '1670ab44-c8ca-433e-8f47-ea0c1ddb7f21',
        sessionTime: '2021-09-29T13:17:27+00:00',
        lastAttempt: {
          id: '1',
          retryAttemptName: null,
          done: true,
          success: true,
          cancelRequested: false,
          params: {},
          createdAt: '2021-09-29T13:17:27Z',
          finishedAt: '2021-09-29T13:17:29Z'
        }
      })
    })
  }
  if (url === '/api/sessions/1/attempts?include_retried=true' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({
        attempts: [
          {
            status: 'success',
            id: '1',
            index: 1,
            project: { id: '1', name: 'example' },
            workflow: { name: 'basic', id: '5' },
            sessionId: '1',
            sessionUuid: '1670ab44-c8ca-433e-8f47-ea0c1ddb7f21',
            sessionTime: '2021-09-29T13:17:27+00:00',
            retryAttemptName: null,
            done: true,
            success: true,
            cancelRequested: false,
            params: {},
            createdAt: '2021-09-29T13:17:27Z',
            finishedAt: '2021-09-29T13:17:29Z'
          }
        ]
      })
    })
  }
  if (url === '/api/attempts/1' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({
        status: 'success',
        id: '1',
        index: 1,
        project: { id: '1', name: 'example' },
        workflow: { name: 'basic', id: '5' },
        sessionId: '1',
        sessionUuid: '1670ab44-c8ca-433e-8f47-ea0c1ddb7f21',
        sessionTime: '2021-09-29T13:17:27+00:00',
        retryAttemptName: null,
        done: true,
        success: true,
        cancelRequested: false,
        params: {},
        createdAt: '2021-09-29T13:17:27Z',
        finishedAt: '2021-09-29T13:17:29Z'
      })
    })
  }
  if (url === '/api/attempts/1/tasks' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({
        tasks: [
          {
            id: '1',
            fullName: '+basic',
            parentId: null,
            config: {},
            upstreams: [],
            state: 'success',
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: '2021-09-29T13:17:29Z',
            retryAt: null,
            startedAt: null,
            error: {},
            isGroup: true
          },
          {
            id: '2',
            fullName: '+basic+my_task_1',
            parentId: '1',
            config: { 'sh>': 'echo this task runs first.' },
            upstreams: [],
            state: 'success',
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: '2021-09-29T13:17:27Z',
            retryAt: null,
            startedAt: '2021-09-29T13:17:27Z',
            error: {},
            isGroup: false
          },
          {
            id: '3',
            fullName: '+basic+my_task_2',
            parentId: '1',
            config: { 'sh>': 'echo this task runs next.' },
            upstreams: ['2'],
            state: 'success',
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: '2021-09-29T13:17:27Z',
            retryAt: null,
            startedAt: '2021-09-29T13:17:27Z',
            error: {},
            isGroup: false
          },
          {
            id: '4',
            fullName: '+basic+any_task_name_here',
            parentId: '1',
            config: {},
            upstreams: ['3'],
            state: 'success',
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: '2021-09-29T13:17:28Z',
            retryAt: null,
            startedAt: null,
            error: {},
            isGroup: true
          },
          {
            id: '5',
            fullName: '+basic+any_task_name_here+nested_task',
            parentId: '4',
            config: { 'sh>': 'echo tasks can be nested like this.' },
            upstreams: [],
            state: 'success',
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: '2021-09-29T13:17:27Z',
            retryAt: null,
            startedAt: '2021-09-29T13:17:27Z',
            error: {},
            isGroup: false
          },
          {
            id: '6',
            fullName: '+basic+any_task_name_here+nested_task_2',
            parentId: '4',
            config: { 'sh>': 'echo nesting is useful for grouping' },
            upstreams: ['5'],
            state: 'success',
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: '2021-09-29T13:17:28Z',
            retryAt: null,
            startedAt: '2021-09-29T13:17:27Z',
            error: {},
            isGroup: false
          },
          {
            id: '7',
            fullName: '+basic+parallel_task_foo',
            parentId: '1',
            config: { _parallel: true },
            upstreams: ['4'],
            state: 'success',
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: '2021-09-29T13:17:29Z',
            retryAt: null,
            startedAt: null,
            error: {},
            isGroup: true
          },
          {
            id: '8',
            fullName: '+basic+parallel_task_foo+bar',
            parentId: '7',
            config: {
              'sh>':
                "echo if 'parallel: true' is set, child tasks run in parallel"
            },
            upstreams: [],
            state: 'success',
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: '2021-09-29T13:17:29Z',
            retryAt: null,
            startedAt: '2021-09-29T13:17:28Z',
            error: {},
            isGroup: false
          },
          {
            id: '9',
            fullName: '+basic+parallel_task_foo+baz',
            parentId: '7',
            config: { 'sh>': 'echo bar and baz run in parallel' },
            upstreams: [],
            state: 'success',
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: '2021-09-29T13:17:29Z',
            retryAt: null,
            startedAt: '2021-09-29T13:17:28Z',
            error: {},
            isGroup: false
          },
          {
            id: '10',
            fullName: '+basic+abc',
            parentId: '1',
            config: {
              'sh>':
                'echo please check other examples in examples/ directory for more features.'
            },
            upstreams: ['7'],
            state: 'success',
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: '2021-09-29T13:17:29Z',
            retryAt: null,
            startedAt: '2021-09-29T13:17:29Z',
            error: {},
            isGroup: false
          }
        ]
      })
    })
  }
  return Promise.reject(new Error(`mock for fetch is undefined. (url: ${url})`))
})
