// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Status, StatusLabel } from '@/api/v1/schema';
import DAGRunRetentionControl from '../DAGRunRetentionControl';

const putMock = vi.fn();
const deleteMock = vi.fn();
const refreshMock = vi.fn();
const showToastMock = vi.fn();
const showErrorMock = vi.fn();
let canWrite = true;

vi.mock('@/hooks/api', () => ({
  useClient: () => ({
    PUT: putMock,
    DELETE: deleteMock,
  }),
}));

vi.mock('@/contexts/AuthContext', () => ({
  useCanWrite: () => canWrite,
}));

vi.mock('@/contexts/RemoteNodeContext', () => ({
  useRemoteNode: () => 'local',
}));

vi.mock('@/components/ui/error-modal', () => ({
  useErrorModal: () => ({ showError: showErrorMock }),
}));

vi.mock('@/components/ui/simple-toast', () => ({
  useSimpleToast: () => ({ showToast: showToastMock }),
}));

const rootRun = (
  overrides: Partial<
    React.ComponentProps<typeof DAGRunRetentionControl>['dagRun']
  > = {}
): React.ComponentProps<typeof DAGRunRetentionControl>['dagRun'] =>
  ({
    name: 'evidence-dag',
    dagRunId: 'run-1',
    rootDAGRunName: 'evidence-dag',
    rootDAGRunId: 'run-1',
    status: Status.Failed,
    statusLabel: StatusLabel.failed,
    startedAt: '2026-01-01T00:00:00Z',
    finishedAt: '2026-01-01T00:01:00Z',
    autoRetryCount: 0,
    ...overrides,
  }) as never;

describe('DAGRunRetentionControl', () => {
  beforeEach(() => {
    putMock.mockReset();
    deleteMock.mockReset();
    refreshMock.mockReset();
    showToastMock.mockReset();
    showErrorMock.mockReset();
    canWrite = true;
  });

  it('renders nothing for active or child runs', () => {
    const { rerender, container } = render(
      <DAGRunRetentionControl
        dagRun={rootRun({ status: Status.Running })}
        refresh={refreshMock}
      />
    );
    expect(container).toBeEmptyDOMElement();

    rerender(
      <DAGRunRetentionControl
        dagRun={rootRun({
          dagRunId: 'child-1',
          rootDAGRunId: 'run-1',
          status: Status.Success,
        })}
        refresh={refreshMock}
      />
    );
    expect(container).toBeEmptyDOMElement();
  });

  it('offers the retain action only to writers', () => {
    const { rerender } = render(
      <DAGRunRetentionControl dagRun={rootRun()} refresh={refreshMock} />
    );
    expect(
      screen.getByRole('button', { name: 'Retain as evidence' })
    ).toBeInTheDocument();

    canWrite = false;
    rerender(
      <DAGRunRetentionControl dagRun={rootRun()} refresh={refreshMock} />
    );
    expect(
      screen.queryByRole('button', { name: 'Retain as evidence' })
    ).not.toBeInTheDocument();
  });

  it('places the hold with the operator reason and refreshes', async () => {
    putMock.mockResolvedValue({ data: {}, error: undefined });

    render(
      <DAGRunRetentionControl dagRun={rootRun()} refresh={refreshMock} />
    );
    fireEvent.click(
      screen.getByRole('button', { name: 'Retain as evidence' })
    );

    const reasonInput = await screen.findByLabelText('Reason (optional)');
    fireEvent.change(reasonInput, {
      target: { value: '  incident INC-9  ' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Retain run' }));

    await waitFor(() => expect(putMock).toHaveBeenCalledTimes(1));
    expect(putMock).toHaveBeenCalledWith(
      '/dag-runs/{name}/{dagRunId}/retention',
      {
        params: {
          path: { name: 'evidence-dag', dagRunId: 'run-1' },
          query: { remoteNode: 'local' },
        },
        body: { reason: 'incident INC-9' },
      }
    );
    expect(refreshMock).toHaveBeenCalledTimes(1);
    expect(showToastMock).toHaveBeenCalledWith(
      'DAG run retained as evidence'
    );
  });

  it('shows the hold with operator metadata and releases it', async () => {
    deleteMock.mockResolvedValue({ data: undefined, error: undefined });

    render(
      <DAGRunRetentionControl
        dagRun={rootRun({
          retention: {
            lockedBy: 'alice',
            lockedAt: '2026-01-02T03:04:05Z',
            reason: 'legal hold',
          },
        })}
        refresh={refreshMock}
      />
    );

    expect(screen.getByText('Retained')).toBeInTheDocument();
    fireEvent.click(
      screen.getByRole('button', { name: 'Release retention' })
    );
    // Confirmation dialog shows the recorded reason and operator.
    expect(await screen.findByText('legal hold')).toBeInTheDocument();
    expect(screen.getByText(/Retained by alice/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Release hold' }));
    await waitFor(() => expect(deleteMock).toHaveBeenCalledTimes(1));
    expect(deleteMock).toHaveBeenCalledWith(
      '/dag-runs/{name}/{dagRunId}/retention',
      {
        params: {
          path: { name: 'evidence-dag', dagRunId: 'run-1' },
          query: { remoteNode: 'local' },
        },
      }
    );
    expect(refreshMock).toHaveBeenCalledTimes(1);
    expect(showToastMock).toHaveBeenCalledWith('Retention hold released');
  });
});
