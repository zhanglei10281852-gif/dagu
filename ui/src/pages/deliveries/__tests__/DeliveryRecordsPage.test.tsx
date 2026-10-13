// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { SWRConfig } from 'swr';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { NotificationDeliveryStatus } from '@/api/v1/schema';
import { useClient } from '@/hooks/api';
import { DeliveryRecordsPage } from '../DeliveryRecordsPage';

vi.mock('@/hooks/api', async () => {
  const { default: useSWR } = await import('swr');
  const useClient = vi.fn();
  return {
    useClient,
    useQuery: (path: string, params: unknown) =>
      useSWR(params === null ? null : [path, JSON.stringify(params)], async () => {
        const result = await useClient().GET(path, params);
        if (result.error) throw result.error;
        return result.data;
      }),
  };
});
vi.mock('@/i18n/I18nText', () => ({
  I18nText: ({ text }: { text: string }) => <span>{text}</span>,
}));
vi.mock('@/components/ui/relative-time', () => ({
  RelativeTime: ({ timestamp, fallback = '-' }: { timestamp?: string | null; fallback?: string }) => (
    <span>{timestamp ?? fallback}</span>
  ),
}));

const useClientMock = vi.mocked(useClient);

function record(overrides: Record<string, unknown>) {
  return {
    recordId: 'rec-1',
    destination: 'route\x00global\x00r1',
    eventKey: 'run-1:attempt-1:failed',
    dagName: 'briefing',
    dagRunId: 'run-1',
    eventType: 'dag.run.failed',
    status: NotificationDeliveryStatus.pending,
    attempts: 2,
    generation: 1,
    lastErrorCategory: 'rate_limited',
    lastErrorSummary: 'HTTP 429: busy',
    createdAt: '2026-10-13T00:00:00Z',
    updatedAt: '2026-10-13T00:05:00Z',
    ...overrides,
  };
}

function renderPage() {
  return render(
    <SWRConfig value={{ provider: () => new Map() }}>
      <DeliveryRecordsPage scope="notifications" />
    </SWRConfig>,
  );
}

describe('DeliveryRecordsPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('lists pending records with attempts and sanitized summary', async () => {
    const GET = vi.fn().mockResolvedValue({
      data: { records: [record({})], nextCursor: null },
    });
    useClientMock.mockReturnValue({ GET, POST: vi.fn() } as never);

    renderPage();

    await waitFor(() => expect(screen.getByText('briefing')).toBeTruthy());
    expect(screen.getByText('2')).toBeTruthy();
    expect(screen.getByText('HTTP 429: busy')).toBeTruthy();
    // Initial tab is pending.
    const firstCall = GET.mock.calls[0]!;
    expect(firstCall[0]).toBe('/notification-deliveries');
    expect((firstCall[1] as { params: { query: { status?: string[] } } }).params.query.status).toEqual([NotificationDeliveryStatus.pending]);
  });

  it('filters dead letters and redrives them with per-record feedback', async () => {
    const GET = vi.fn().mockImplementation((path: string, params: unknown) => {
      const status = (params as { params: { query: { status?: string[] } } }).params.query.status;
      if (status?.[0] === NotificationDeliveryStatus.dead_letter) {
        return Promise.resolve({
          data: {
            records: [
              record({
                recordId: 'rec-dead',
                status: NotificationDeliveryStatus.dead_letter,
                attempts: 10,
                deadLetterReason: 'attempt_limit',
              }),
            ],
            nextCursor: null,
          },
        });
      }
      return Promise.resolve({ data: { records: [], nextCursor: null } });
    });
    const POST = vi.fn().mockResolvedValue({
      data: {
        results: [
          {
            recordId: 'rec-dead',
            outcome: 'redriven',
            status: NotificationDeliveryStatus.pending,
            generation: 2,
          },
        ],
      },
    });
    useClientMock.mockReturnValue({ GET, POST } as never);

    const user = userEvent.setup();
    renderPage();

    await user.click(screen.getByText('Dead letter'));
    await waitFor(() => expect(screen.getByText('10')).toBeTruthy());

    // Row-level redrive posts the opaque record id.
    await user.click(screen.getAllByText('Redrive')[0]!);
    await waitFor(() => expect(POST).toHaveBeenCalledTimes(1));
    const postCall = POST.mock.calls[0]!;
    expect(postCall[0]).toBe('/notification-deliveries/redrive');
    expect(postCall[1].body.recordIds).toEqual(['rec-dead']);
    await waitFor(() => expect(screen.getByTestId('redrive-summary').textContent).toContain('1 redriven'));
  });

  it('supports bulk selection of dead letters', async () => {
    const GET = vi.fn().mockResolvedValue({
      data: {
        records: [
          record({ recordId: 'rec-a', status: NotificationDeliveryStatus.dead_letter }),
          record({ recordId: 'rec-b', eventKey: 'run-2', status: NotificationDeliveryStatus.dead_letter }),
        ],
        nextCursor: null,
      },
    });
    const POST = vi.fn().mockResolvedValue({
      data: {
        results: [
          { recordId: 'rec-a', outcome: 'redriven', status: 'pending', generation: 2 },
          { recordId: 'rec-b', outcome: 'redriven', status: 'pending', generation: 2 },
        ],
      },
    });
    useClientMock.mockReturnValue({ GET, POST } as never);

    const user = userEvent.setup();
    renderPage();

    // Open dead letter tab to see checkboxes.
    await user.click(screen.getByText('Dead letter'));
    await waitFor(() => expect(screen.getAllByRole('checkbox').length).toBeGreaterThan(1));

    const selectAll = screen.getByLabelText('Select all dead letters');
    await user.click(selectAll);
    const bulkButton = screen.getByTestId('redrive-selected');
    await user.click(bulkButton);

    await waitFor(() => expect(POST).toHaveBeenCalledTimes(1));
    const bulkCall = POST.mock.calls[0]!;
    expect(bulkCall[1].body.recordIds.sort()).toEqual(['rec-a', 'rec-b']);
    await waitFor(() => expect(within(screen.getByTestId('redrive-summary')).getByText(/2 redriven/)).toBeTruthy());
  });

  it('uses incident endpoints for the incident scope', async () => {
    const GET = vi.fn().mockResolvedValue({ data: { records: [], nextCursor: null } });
    useClientMock.mockReturnValue({ GET, POST: vi.fn() } as never);

    render(
      <SWRConfig value={{ provider: () => new Map() }}>
        <DeliveryRecordsPage scope="incidents" />
      </SWRConfig>,
    );

    await waitFor(() => expect(GET).toHaveBeenCalled());
    expect(GET.mock.calls[0]![0]).toBe('/incident-deliveries');
  });
});
