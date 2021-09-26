import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { getByText, queryByText, screen, render, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom/extend-expect';

import { SessionPage, AttemptPage } from './console';

global.fetch = jest.fn((url, { method }) => {
  if (url === '/api/workflows/5' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({
        id: "5",
        name: "basic",
        project: { id: "1", name: "example" },
        revision: "d232a92c-09dc-4eba-815d-f95d18dc3d7f",
        timezone: "UTC",
        config: {
          "+my_task_1": { "sh>": "echo this task runs first." },
          "+my_task_2": { "sh>": "echo this task runs next." },
          "+any_task_name_here": {
            "+nested_task": { "sh>": "echo tasks can be nested like this." },
            "+nested_task_2": { "sh>": "echo nesting is useful for grouping" },
          },
          "+parallel_task_foo": {
            _parallel: true,
            "+bar": {
              "sh>":
                "echo if 'parallel: true' is set, child tasks run in parallel",
            },
            "+baz": { "sh>": "echo bar and baz run in parallel" },
          },
          "+abc": {
            "sh>":
              "echo please check other examples in examples/ directory for more features.",
          },
        },
      }),
    });
  }
  if (url === '/api/sessions/1' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({
        id: "1",
        project: { id: "1", name: "example" },
        workflow: { name: "basic", id: "5" },
        sessionUuid: "1670ab44-c8ca-433e-8f47-ea0c1ddb7f21",
        sessionTime: "2021-09-29T13:17:27+00:00",
        lastAttempt: {
          id: "1",
          retryAttemptName: null,
          done: true,
          success: true,
          cancelRequested: false,
          params: {},
          createdAt: "2021-09-29T13:17:27Z",
          finishedAt: "2021-09-29T13:17:29Z",
        },
      }),
    });
  }
  if (url === '/api/sessions/1/attempts?include_retried=true' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({
        attempts: [
          {
            status: "success",
            id: "1",
            index: 1,
            project: { id: "1", name: "example" },
            workflow: { name: "basic", id: "5" },
            sessionId: "1",
            sessionUuid: "1670ab44-c8ca-433e-8f47-ea0c1ddb7f21",
            sessionTime: "2021-09-29T13:17:27+00:00",
            retryAttemptName: null,
            done: true,
            success: true,
            cancelRequested: false,
            params: {},
            createdAt: "2021-09-29T13:17:27Z",
            finishedAt: "2021-09-29T13:17:29Z",
          },
        ],
      }),
    });
  }
  if (url === '/api/attempts/1' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({
        status: "success",
        id: "1",
        index: 1,
        project: { id: "1", name: "example" },
        workflow: { name: "basic", id: "5" },
        sessionId: "1",
        sessionUuid: "1670ab44-c8ca-433e-8f47-ea0c1ddb7f21",
        sessionTime: "2021-09-29T13:17:27+00:00",
        retryAttemptName: null,
        done: true,
        success: true,
        cancelRequested: false,
        params: {},
        createdAt: "2021-09-29T13:17:27Z",
        finishedAt: "2021-09-29T13:17:29Z",
      }),
    });
  }
  if (url === '/api/attempts/1/tasks' && method === 'GET') {
    return Promise.resolve({
      ok: true,
      status: 200,
      json: () => ({
        tasks: [
          {
            id: "1",
            fullName: "+basic",
            parentId: null,
            config: {},
            upstreams: [],
            state: "success",
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: "2021-09-29T13:17:29Z",
            retryAt: null,
            startedAt: null,
            error: {},
            isGroup: true,
          },
          {
            id: "2",
            fullName: "+basic+my_task_1",
            parentId: "1",
            config: { "sh>": "echo this task runs first." },
            upstreams: [],
            state: "success",
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: "2021-09-29T13:17:27Z",
            retryAt: null,
            startedAt: "2021-09-29T13:17:27Z",
            error: {},
            isGroup: false,
          },
          {
            id: "3",
            fullName: "+basic+my_task_2",
            parentId: "1",
            config: { "sh>": "echo this task runs next." },
            upstreams: ["2"],
            state: "success",
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: "2021-09-29T13:17:27Z",
            retryAt: null,
            startedAt: "2021-09-29T13:17:27Z",
            error: {},
            isGroup: false,
          },
          {
            id: "4",
            fullName: "+basic+any_task_name_here",
            parentId: "1",
            config: {},
            upstreams: ["3"],
            state: "success",
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: "2021-09-29T13:17:28Z",
            retryAt: null,
            startedAt: null,
            error: {},
            isGroup: true,
          },
          {
            id: "5",
            fullName: "+basic+any_task_name_here+nested_task",
            parentId: "4",
            config: { "sh>": "echo tasks can be nested like this." },
            upstreams: [],
            state: "success",
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: "2021-09-29T13:17:27Z",
            retryAt: null,
            startedAt: "2021-09-29T13:17:27Z",
            error: {},
            isGroup: false,
          },
          {
            id: "6",
            fullName: "+basic+any_task_name_here+nested_task_2",
            parentId: "4",
            config: { "sh>": "echo nesting is useful for grouping" },
            upstreams: ["5"],
            state: "success",
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: "2021-09-29T13:17:28Z",
            retryAt: null,
            startedAt: "2021-09-29T13:17:27Z",
            error: {},
            isGroup: false,
          },
          {
            id: "7",
            fullName: "+basic+parallel_task_foo",
            parentId: "1",
            config: { _parallel: true },
            upstreams: ["4"],
            state: "success",
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: "2021-09-29T13:17:29Z",
            retryAt: null,
            startedAt: null,
            error: {},
            isGroup: true,
          },
          {
            id: "8",
            fullName: "+basic+parallel_task_foo+bar",
            parentId: "7",
            config: {
              "sh>":
                "echo if 'parallel: true' is set, child tasks run in parallel",
            },
            upstreams: [],
            state: "success",
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: "2021-09-29T13:17:29Z",
            retryAt: null,
            startedAt: "2021-09-29T13:17:28Z",
            error: {},
            isGroup: false,
          },
          {
            id: "9",
            fullName: "+basic+parallel_task_foo+baz",
            parentId: "7",
            config: { "sh>": "echo bar and baz run in parallel" },
            upstreams: [],
            state: "success",
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: "2021-09-29T13:17:29Z",
            retryAt: null,
            startedAt: "2021-09-29T13:17:28Z",
            error: {},
            isGroup: false,
          },
          {
            id: "10",
            fullName: "+basic+abc",
            parentId: "1",
            config: {
              "sh>":
                "echo please check other examples in examples/ directory for more features.",
            },
            upstreams: ["7"],
            state: "success",
            cancelRequested: false,
            exportParams: {},
            storeParams: {},
            stateParams: {},
            updatedAt: "2021-09-29T13:17:29Z",
            retryAt: null,
            startedAt: "2021-09-29T13:17:29Z",
            error: {},
            isGroup: false,
          },
        ],
      }),
    });
  }
  return Promise.reject(new Error(`mock for fetch is undefined. (url: ${url})`));
})

function getSection(headingText: string): HTMLElement {
  return screen.getByText(headingText, { selector: "h2" }).closest("div");
}

describe('SessionPage and AttemptPage', () => {
  describe('SessionPage', () => {
    const match = {
      params: {
        sessionId: 1,
      },
    };
    
    beforeEach(() => {
      render(
        <MemoryRouter>
          <SessionPage match={match} />
        </MemoryRouter>
      );
    });

    it("SessionView", () => {
      const sessionSection = getSection("Session")
      expect(getByText(sessionSection, "example").closest('a').href).toBe('http://localhost/projects/1');
      expect(getByText(sessionSection, "basic").closest('a').href).toBe('http://localhost/workflows/5');
      expect(getByText(sessionSection, "Success").closest('a').href).toBe('http://localhost/attempts/1');
    });

    commonTests();
  })

  describe('AttemptPage', () => {
    const match = {
      params: {
        attemptId: 1,
      },
    };
    
    beforeEach(() => {
      render(
        <MemoryRouter>
          <AttemptPage match={match} />
        </MemoryRouter>
      );
    });

    it("AttemptView", () => {
      const sessionSection = getSection("Attempt")
      expect(getByText(sessionSection, "example").closest('a').href).toBe('http://localhost/projects/1');
      expect(getByText(sessionSection, "basic").closest('a').href).toBe('http://localhost/workflows/5');
      expect(getByText(sessionSection, "Success")).toBeInTheDocument();
    });

    commonTests();
  })

  function commonTests() {
    it("AttemptTimelineView", () => {
      const timelineSection = getSection("Timeline");
      expect(getByText(timelineSection, "+my_task_1")).toBeInTheDocument();
      expect(getByText(timelineSection, "+my_task_2")).toBeInTheDocument();
      expect(getByText(timelineSection, "+any_task_name_here")).toBeInTheDocument();
      expect(getByText(timelineSection, "+nested_task")).toBeInTheDocument();
      expect(getByText(timelineSection, "+nested_task_2")).toBeInTheDocument();
      expect(getByText(timelineSection, "+parallel_task_foo")).toBeInTheDocument();
      expect(getByText(timelineSection, "+bar")).toBeInTheDocument();
      expect(getByText(timelineSection, "+baz")).toBeInTheDocument();
      expect(getByText(timelineSection, "+abc")).toBeInTheDocument();

      // root tasks are not rendered
      expect(queryByText(timelineSection, "+basic")).not.toBeInTheDocument();
    })

    it("AttemptTasksView", () => {
      const timelineSection = getSection("Tasks");
      expect(getByText(timelineSection, "+basic")).toBeInTheDocument();
      expect(getByText(timelineSection, "+basic+my_task_1")).toBeInTheDocument();
      expect(getByText(timelineSection, "+basic+my_task_2")).toBeInTheDocument();
      expect(getByText(timelineSection, "+basic+any_task_name_here")).toBeInTheDocument();
      expect(getByText(timelineSection, "+basic+any_task_name_here+nested_task")).toBeInTheDocument();
      expect(getByText(timelineSection, "+basic+any_task_name_here+nested_task_2")).toBeInTheDocument();
      expect(getByText(timelineSection, "+basic+parallel_task_foo")).toBeInTheDocument();
      expect(getByText(timelineSection, "+basic+parallel_task_foo+bar")).toBeInTheDocument();
      expect(getByText(timelineSection, "+basic+parallel_task_foo+baz")).toBeInTheDocument();
      expect(getByText(timelineSection, "+basic+abc")).toBeInTheDocument();
    })
  }
})