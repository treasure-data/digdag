import React from 'react'
import { MemoryRouter, Route, Switch } from 'react-router-dom'
import {
  getByText,
  getByLabelText,
  fireEvent,
  queryByText,
  screen,
  render
} from '@testing-library/react'
import '@testing-library/jest-dom/extend-expect'

import { SessionPage, AttemptPage, WorkflowsPage } from './console'

function getSection (headingText: string): HTMLElement {
  // eslint-disable-next-line  @typescript-eslint/no-non-null-assertion
  return screen.getByText(headingText, { selector: 'h2' }).closest('div')!
}

describe('SessionPage and AttemptPage', () => {
  describe('SessionPage', () => {
    beforeEach(() => {
      render(
        <MemoryRouter initialEntries={['/sessions/1']}>
          <Switch>
            <Route path='/sessions/:sessionId' component={SessionPage} />
          </Switch>
        </MemoryRouter>
      )
    })

    it('SessionView', () => {
      const sessionSection = getSection('Session')
      expect(getByText(sessionSection, 'example').closest('a')?.href).toBe('http://localhost/projects/1')
      expect(getByText(sessionSection, 'basic').closest('a')?.href).toBe('http://localhost/workflows/5')
      expect(getByText(sessionSection, 'Success').closest('a')?.href).toBe('http://localhost/attempts/1')
    })

    commonTests()
  })

  describe('AttemptPage', () => {
    beforeEach(() => {
      render(
        <MemoryRouter initialEntries={['/attempts/1']}>
          <Switch>
            <Route path='/attempts/:attemptId' component={AttemptPage} />
          </Switch>
        </MemoryRouter>
      )
    })

    it('AttemptView', () => {
      const sessionSection = getSection('Attempt')
      expect(getByText(sessionSection, 'example').closest('a')?.href).toBe('http://localhost/projects/1')
      expect(getByText(sessionSection, 'basic').closest('a')?.href).toBe('http://localhost/workflows/5')
      expect(getByText(sessionSection, 'Success')).toBeInTheDocument()
    })

    commonTests()
  })

  function commonTests (): void {
    describe('AttemptTimelineView', () => {
      it('tasks are rendered', () => {
        const timelineSection = getSection('Timeline')
        expect(getByText(timelineSection, '+my_task_1')).toBeInTheDocument()
        expect(getByText(timelineSection, '+my_task_2')).toBeInTheDocument()
        expect(getByText(timelineSection, '+any_task_name_here')).toBeInTheDocument()
        expect(getByText(timelineSection, '+nested_task')).toBeInTheDocument()
        expect(getByText(timelineSection, '+nested_task_2')).toBeInTheDocument()
        expect(getByText(timelineSection, '+parallel_task_foo')).toBeInTheDocument()
        expect(getByText(timelineSection, '+bar')).toBeInTheDocument()
        expect(getByText(timelineSection, '+baz')).toBeInTheDocument()
        expect(getByText(timelineSection, '+abc')).toBeInTheDocument()

        // root tasks are not rendered
        expect(queryByText(timelineSection, '+basic')).not.toBeInTheDocument()
      })

      it('subtasks folds on click', () => {
        const timelineSection = getSection('Timeline')
        expect(getByText(timelineSection, '+nested_task')).toBeInTheDocument()
        expect(getByText(timelineSection, '+nested_task_2')).toBeInTheDocument()

        fireEvent.click(getByText(timelineSection, '+any_task_name_here'))

        expect(queryByText(timelineSection, '+nested_task')).not.toBeInTheDocument()
        expect(queryByText(timelineSection, '+nested_task_2')).not.toBeInTheDocument()

        fireEvent.click(getByText(timelineSection, '+any_task_name_here'))

        expect(getByText(timelineSection, '+nested_task')).toBeInTheDocument()
        expect(getByText(timelineSection, '+nested_task_2')).toBeInTheDocument()
      })
    })

    it('AttemptTasksView', () => {
      const timelineSection = getSection('Tasks')
      expect(getByText(timelineSection, '+basic')).toBeInTheDocument()
      expect(getByText(timelineSection, '+basic+my_task_1')).toBeInTheDocument()
      expect(getByText(timelineSection, '+basic+my_task_2')).toBeInTheDocument()
      expect(getByText(timelineSection, '+basic+any_task_name_here')).toBeInTheDocument()
      expect(getByText(timelineSection, '+basic+any_task_name_here+nested_task')).toBeInTheDocument()
      expect(getByText(timelineSection, '+basic+any_task_name_here+nested_task_2')).toBeInTheDocument()
      expect(getByText(timelineSection, '+basic+parallel_task_foo')).toBeInTheDocument()
      expect(getByText(timelineSection, '+basic+parallel_task_foo+bar')).toBeInTheDocument()
      expect(getByText(timelineSection, '+basic+parallel_task_foo+baz')).toBeInTheDocument()
      expect(getByText(timelineSection, '+basic+abc')).toBeInTheDocument()
    })
  }
})

describe('WorkflowsPage', () => {
  beforeEach(() => {
    render(
      <MemoryRouter>
        <WorkflowsPage />
      </MemoryRouter>
    )
  })

  it('WorkflowsView', () => {
    const workflowsSection = getSection('Workflows')
    expect(getByText(workflowsSection, 'error_task').closest('a')?.href).toBe('http://localhost/projects/1/workflows/error_task')
    expect(getByText(workflowsSection, 'generate_subtasks').closest('a')?.href).toBe('http://localhost/projects/1/workflows/generate_subtasks')
  })

  describe('SessionsView', () => {
    it('initial contents', () => {
      const sessionsSection = getSection('Sessions')

      expect(getByText(sessionsSection, 'example')).toBeInTheDocument()
      expect(getByText(sessionsSection, 'basic')).toBeInTheDocument()
      expect(getByText(sessionsSection, 'd232a92c-09dc-4eba-815d-f95d18dc3d7f')).toBeInTheDocument()
    })

    it('status filter', () => {
      const sessionsSection = getSection('Sessions')

      expect(getByText(sessionsSection, 'basic')).toBeInTheDocument()

      fireEvent.change(getByLabelText(sessionsSection, 'Status:'), { target: { value: 'Failure' } })
      expect(queryByText(sessionsSection, 'basic')).not.toBeInTheDocument()

      fireEvent.change(getByLabelText(sessionsSection, 'Status:'), { target: { value: 'Success' } })
      expect(getByText(sessionsSection, 'basic')).toBeInTheDocument()
    })
  })
})
