// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { components, QueueType } from '@/api/v1/schema';
import { AppBarContext } from '@/contexts/AppBarContext';
import QueueDetailsPage from '..';

const { clientMock, getMock, postMock, useQueryMock } = vi.hoisted(() => {
  const get = vi.fn();
  const post = vi.fn();
  return {
    clientMock: { GET: get, POST: post },
    getMock: get,
    postMock: post,
    useQueryMock: vi.fn(),
  };
});

vi.mock('@/hooks/api', () => ({
  useClient: () => clientMock,
  useQuery: useQueryMock,
}));

vi.mock('@/contexts/AuthContext', () => ({
  useCanExecute: () => true,
}));

vi.mock('@/features/dag-runs/components/dag-run-details', () => ({
  DAGRunDetailsModal: () => null,
}));

const cappedZeroQueue: components['schemas']['Queue'] = {
  name: 'running-heavy-q',
  type: QueueType.dag_based,
  maxConcurrency: 1,
  runningCount: 0,
  queuedCount: 0,
  queuedCountCapped: true,
  running: [],
};

function renderPage(queue: components['schemas']['Queue']): void {
  useQueryMock.mockReturnValue({
    data: queue,
    error: null,
    isLoading: false,
    isValidating: false,
    mutate: vi.fn(),
  });

  render(
    <MemoryRouter initialEntries={[`/queues/${queue.name}`]}>
      <AppBarContext.Provider
        value={
          {
            selectedRemoteNode: 'local',
            setTitle: vi.fn(),
          } as never
        }
      >
        <Routes>
          <Route path="/queues/:name" element={<QueueDetailsPage />} />
        </Routes>
      </AppBarContext.Provider>
    </MemoryRouter>
  );
}

describe('QueueDetailsPage', () => {
  beforeEach(() => {
    getMock.mockReset();
    postMock.mockReset();
    useQueryMock.mockReset();
    postMock.mockResolvedValue({ error: undefined });
    getMock.mockResolvedValue({
      data: { items: [] },
      error: undefined,
    });
  });

  it('loads items when a capped lower-bound count is zero', async () => {
    getMock.mockResolvedValueOnce({
      data: { items: [], nextCursor: 'cursor-1' },
      error: undefined,
    });

    renderPage(cappedZeroQueue);

    await waitFor(() => {
      expect(getMock).toHaveBeenCalledWith(
        '/queues/{name}/items',
        expect.any(Object)
      );
    });
    expect(
      await screen.findByText(
        'No visible queued items were returned for this queue.'
      )
    ).toBeVisible();
    expect(
      screen.queryByText('No queued items in this queue.')
    ).not.toBeInTheDocument();

    expect(getMock).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('button', { name: 'Continue scanning' }));

    await waitFor(() => {
      expect(getMock).toHaveBeenCalledTimes(2);
    });
    expect(getMock).toHaveBeenNthCalledWith(
      2,
      '/queues/{name}/items',
      expect.objectContaining({
        params: expect.objectContaining({
          query: expect.objectContaining({ cursor: 'cursor-1' }),
        }),
      })
    );
  });

  it('does not load items when an exact count is zero', async () => {
    renderPage({ ...cappedZeroQueue, queuedCountCapped: undefined });

    expect(
      await screen.findByText('No queued items in this queue.')
    ).toBeVisible();
    expect(getMock).not.toHaveBeenCalled();
  });

  it('pauses the queue with an optional reason', async () => {
    renderPage(cappedZeroQueue);

    fireEvent.click(await screen.findByRole('button', { name: 'Pause queue' }));
    const reasonInput = await screen.findByLabelText('Reason (optional)');
    fireEvent.change(reasonInput, { target: { value: 'downstream work' } });
    fireEvent.click(screen.getByRole('button', { name: 'Pause queue' }));

    await waitFor(() => {
      expect(postMock).toHaveBeenCalledTimes(1);
    });
    expect(postMock).toHaveBeenCalledWith(
      '/queues/{name}/pause',
      expect.objectContaining({
        params: expect.objectContaining({
          path: { name: cappedZeroQueue.name },
        }),
        body: { paused: true, reason: 'downstream work' },
      })
    );
  });

  it('shows the pause banner and resumes a paused queue', async () => {
    renderPage({
      ...cappedZeroQueue,
      pause: {
        paused: true,
        reason: 'maintenance',
        pausedBy: 'alice',
        pausedAt: '2026-10-13T01:00:00Z',
      },
    });

    expect(
      await screen.findByText(
        'Queue paused. No new runs start; queued runs are kept in order.'
      )
    ).toBeVisible();
    const banner = await screen.findByRole('status');
    expect(banner.textContent).toContain('maintenance');
    expect(banner.textContent).toContain('alice');

    fireEvent.click(screen.getByRole('button', { name: 'Resume queue' }));
    await waitFor(() => {
      expect(postMock).toHaveBeenCalledTimes(1);
    });
    expect(postMock).toHaveBeenCalledWith(
      '/queues/{name}/pause',
      expect.objectContaining({ body: { paused: false } })
    );
  });
});
