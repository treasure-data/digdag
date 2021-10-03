import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { getByText, queryByText, screen, render, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom/extend-expect';

import { SessionPage, AttemptPage } from './console';

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