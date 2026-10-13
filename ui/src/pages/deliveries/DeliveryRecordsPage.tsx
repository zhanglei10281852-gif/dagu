// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

import { useCallback, useEffect, useMemo, useState } from 'react';
import type { components } from '@/api/v1/schema';
import {
  NotificationDeliveryRedriveOutcome,
  NotificationDeliveryStatus,
} from '@/api/v1/schema';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Tab, Tabs } from '@/components/ui/tabs';
import { I18nText } from '@/i18n/I18nText';
import { useClient, useQuery } from '@/hooks/api';
import { RelativeTime } from '@/components/ui/relative-time';
import { RefreshCw, RotateCcw, Search } from 'lucide-react';

type DeliveryRecord = components['schemas']['NotificationDeliveryRecord'];
type DeliveryStatus = NotificationDeliveryStatus;
type RedriveResult = components['schemas']['NotificationDeliveryRedriveResult'];

const PAGE_SIZE = 25;

const STATUS_TABS: { value: DeliveryStatus | 'all'; label: string }[] = [
  { value: NotificationDeliveryStatus.pending, label: 'Pending' },
  { value: NotificationDeliveryStatus.dead_letter, label: 'Dead letter' },
  { value: NotificationDeliveryStatus.delivered, label: 'Delivered' },
  { value: 'all', label: 'All' },
];

type StatusFilter = DeliveryStatus | 'all';

const STATUS_BADGE_VARIANT: Record<DeliveryStatus, 'running' | 'success' | 'failed'> = {
  [NotificationDeliveryStatus.pending]: 'running',
  [NotificationDeliveryStatus.delivered]: 'success',
  [NotificationDeliveryStatus.dead_letter]: 'failed',
};

export type DeliveryScope = 'notifications' | 'incidents';

function scopePaths(scope: DeliveryScope) {
  if (scope === 'incidents') {
    return {
      list: '/incident-deliveries',
      redrive: '/incident-deliveries/redrive',
    } as const;
  }
  return {
    list: '/notification-deliveries',
    redrive: '/notification-deliveries/redrive',
  } as const;
}

function statusLabel(status: DeliveryStatus): string {
  if (status === NotificationDeliveryStatus.dead_letter) return 'Dead letter';
  const value = String(status);
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function categoryLabel(category?: string): string {
  if (!category) return '-';
  return category.replace(/_/g, ' ');
}

function errorMessageFromBody(body: unknown, fallback: string): string {
  if (body && typeof body === 'object' && 'message' in body) {
    const message = (body as { message?: unknown }).message;
    if (typeof message === 'string') {
      return message;
    }
  }
  return fallback;
}

export function DeliveryRecordsPage({ scope }: { scope: DeliveryScope }) {
  const paths = useMemo(() => scopePaths(scope), [scope]);
  const client = useClient();

  const [statusFilter, setStatusFilter] = useState<StatusFilter>(NotificationDeliveryStatus.pending);
  const [destinationInput, setDestinationInput] = useState('');
  const [destinationFilter, setDestinationFilter] = useState('');
  const [cursor, setCursor] = useState<string | undefined>(undefined);
  const [records, setRecords] = useState<DeliveryRecord[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [detail, setDetail] = useState<DeliveryRecord | null>(null);
  const [redriving, setRedriving] = useState(false);
  const [redriveSummary, setRedriveSummary] = useState<string | null>(null);

  const queryParams = useMemo(() => {
    const query: Record<string, unknown> = { limit: PAGE_SIZE };
    if (statusFilter !== 'all') {
      query.status = [statusFilter];
    }
    if (destinationFilter) {
      query.destination = destinationFilter;
    }
    if (cursor) {
      query.cursor = cursor;
    }
    return { query };
  }, [statusFilter, destinationFilter, cursor]);

  // Notification and incident list endpoints declare distinct (identically
  // valued) query enums; the cast bridges the nominal types.
  const { data, error, isLoading, mutate } = useQuery(
    paths.list,
    { params: queryParams } as never,
  );

  // Merge pages into a deduplicated list keyed by the opaque record id.
  useEffect(() => {
    if (!data) {
      return;
    }
    const pageRecords = (data.records ?? []) as DeliveryRecord[];
    setRecords((previous) => {
      if (!cursor) {
        return pageRecords;
      }
      const merged = new Map<string, DeliveryRecord>();
      for (const record of previous) {
        merged.set(record.recordId, record);
      }
      for (const record of pageRecords) {
        merged.set(record.recordId, record);
      }
      return Array.from(merged.values());
    });
    setNextCursor(data.nextCursor ?? null);
  }, [data, cursor]);

  const resetPaging = useCallback((status?: StatusFilter) => {
    setCursor(undefined);
    setRecords([]);
    setNextCursor(null);
    setSelected(new Set());
    setRedriveSummary(null);
    if (status !== undefined) {
      setStatusFilter(status);
    }
  }, []);

  const applyDestinationFilter = () => {
    setDestinationFilter(destinationInput.trim());
    setCursor(undefined);
  };

  const toggleSelected = (recordId: string) => {
    setSelected((previous) => {
      const next = new Set(previous);
      if (next.has(recordId)) {
        next.delete(recordId);
      } else {
        next.add(recordId);
      }
      return next;
    });
  };

  const redrive = async (recordIds: string[]) => {
    if (recordIds.length === 0) {
      return;
    }
    setRedriving(true);
    setRedriveSummary(null);
    try {
      const { data: responseData, error: responseError } = await client.POST(paths.redrive, {
        body: { recordIds },
      });
      if (responseError) {
        setRedriveSummary(errorMessageFromBody(responseError, 'Redrive failed'));
        return;
      }
      const results = (responseData?.results ?? []) as RedriveResult[];
      const counts = results.reduce(
        (acc, result) => {
          if (result.outcome === NotificationDeliveryRedriveOutcome.redriven) {
            acc.redriven += 1;
          } else if (result.outcome === NotificationDeliveryRedriveOutcome.already_pending) {
            acc.alreadyPending += 1;
          } else if (result.outcome === NotificationDeliveryRedriveOutcome.delivered) {
            acc.delivered += 1;
          } else {
            acc.notFound += 1;
          }
          return acc;
        },
        { redriven: 0, alreadyPending: 0, delivered: 0, notFound: 0 },
      );
      const summary = [
        counts.redriven ? `${counts.redriven} redriven` : '',
        counts.alreadyPending ? `${counts.alreadyPending} already pending` : '',
        counts.delivered ? `${counts.delivered} delivered` : '',
        counts.notFound ? `${counts.notFound} not found` : '',
      ]
        .filter(Boolean)
        .join(', ');
      setRedriveSummary(summary || 'No records were redriven');
      setSelected(new Set());
      await mutate();
    } finally {
      setRedriving(false);
    }
  };

  const selectableRecords = records.filter((record) => record.status === NotificationDeliveryStatus.dead_letter);
  const allSelectableSelected =
    selectableRecords.length > 0 && selectableRecords.every((record) => selected.has(record.recordId));

  return (
    <div className="flex h-full min-h-0 flex-col gap-5 overflow-auto p-6" data-testid={`${scope}-delivery-page`}>
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-lg font-semibold text-foreground">
            <I18nText
              text={scope === 'incidents' ? 'Incident deliveries' : 'Notification deliveries'}
            />
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">
            <I18nText text="Pending retries, backoff schedule, dead letters, and redrive controls." />
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => mutate()} disabled={isLoading}>
            <RefreshCw className="mr-2 size-4" />
            <I18nText text="Refresh" />
          </Button>
          {selected.size > 0 && (
            <Button
              size="sm"
              disabled={redriving}
              onClick={() => redrive(Array.from(selected))}
              data-testid="redrive-selected"
            >
              <RotateCcw className="mr-2 size-4" />
              <I18nText text={`Redrive selected (${selected.size})`} />
            </Button>
          )}
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <Tabs>
          {STATUS_TABS.map((tab) => (
            <Tab
              key={tab.value}
              isActive={statusFilter === tab.value}
              onClick={() => resetPaging(tab.value)}
            >
              <I18nText text={tab.label} />
            </Tab>
          ))}
        </Tabs>
        <div className="flex items-center gap-2">
          <Input
            value={destinationInput}
            onChange={(event) => setDestinationInput(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                applyDestinationFilter();
              }
            }}
            placeholder="Destination"
            className="w-64"
            aria-label="Filter by destination"
          />
          <Button variant="outline" size="sm" onClick={applyDestinationFilter}>
            <Search className="size-4" />
          </Button>
        </div>
      </div>

      {redriveSummary && (
        <div className="rounded-sm border border-border bg-muted px-3 py-2 text-sm" data-testid="redrive-summary">
          {redriveSummary}
        </div>
      )}
      {error && (
        <div className="rounded-sm border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {errorMessageFromBody(error, 'Failed to load delivery records')}
        </div>
      )}

      <div className="rounded-md border border-border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-10">
                <input
                  type="checkbox"
                  aria-label="Select all dead letters"
                  checked={allSelectableSelected}
                  onChange={(event) => {
                    setSelected(event.target.checked ? new Set(selectableRecords.map((r) => r.recordId)) : new Set());
                  }}
                />
              </TableHead>
              <TableHead><I18nText text="DAG" /></TableHead>
              <TableHead><I18nText text="Destination" /></TableHead>
              <TableHead><I18nText text="Status" /></TableHead>
              <TableHead><I18nText text="Attempts" /></TableHead>
              <TableHead><I18nText text="Last error" /></TableHead>
              <TableHead><I18nText text="Last attempt" /></TableHead>
              <TableHead><I18nText text="Next attempt" /></TableHead>
              <TableHead><I18nText text="Terminal at" /></TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            {records.map((record) => (
              <TableRow key={record.recordId} data-testid="delivery-row">
                <TableCell>
                  {record.status === NotificationDeliveryStatus.dead_letter && (
                    <input
                      type="checkbox"
                      aria-label={`Select ${record.recordId}`}
                      checked={selected.has(record.recordId)}
                      onChange={() => toggleSelected(record.recordId)}
                    />
                  )}
                </TableCell>
                <TableCell className="font-mono text-xs">
                  <div>{record.dagName || record.eventKey}</div>
                  {record.dagRunId && <div className="text-muted-foreground">{record.dagRunId}</div>}
                </TableCell>
                <TableCell className="max-w-[220px] truncate font-mono text-xs" title={record.destination}>
                  {record.destination}
                </TableCell>
                <TableCell>
                  <Badge variant={STATUS_BADGE_VARIANT[record.status]}>
                    {statusLabel(record.status)}
                  </Badge>
                </TableCell>
                <TableCell>{record.attempts}</TableCell>
                <TableCell className="max-w-[260px] truncate text-xs" title={record.lastErrorSummary ?? ''}>
                  {record.lastErrorCategory && (
                    <span className="mr-1 text-muted-foreground">{categoryLabel(record.lastErrorCategory)}:</span>
                  )}
                  {record.lastErrorSummary ?? '-'}
                </TableCell>
                <TableCell className="text-xs">
                  <RelativeTime timestamp={record.lastAttemptAt ?? undefined} />
                </TableCell>
                <TableCell className="text-xs">
                  {record.status === NotificationDeliveryStatus.pending && record.nextAttemptAt ? (
                    <RelativeTime timestamp={record.nextAttemptAt} />
                  ) : (
                    '-'
                  )}
                </TableCell>
                <TableCell className="text-xs">
                  <RelativeTime timestamp={record.deliveredAt ?? record.deadLetterAt ?? undefined} />
                </TableCell>
                <TableCell>
                  <div className="flex items-center gap-1">
                    {record.status === NotificationDeliveryStatus.dead_letter && (
                      <Button
                        variant="outline"
                        size="sm"
                        disabled={redriving}
                        onClick={() => redrive([record.recordId])}
                      >
                        <RotateCcw className="mr-1 size-3" />
                        <I18nText text="Redrive" />
                      </Button>
                    )}
                    <Button variant="ghost" size="sm" onClick={() => setDetail(record)}>
                      <I18nText text="Details" />
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
            {!isLoading && records.length === 0 && (
              <TableRow>
                <TableCell colSpan={10} className="py-8 text-center text-sm text-muted-foreground">
                  <I18nText text="No delivery records" />
                </TableCell>
              </TableRow>
            )}
            {isLoading && records.length === 0 && (
              <TableRow>
                <TableCell colSpan={10} className="py-8 text-center text-sm text-muted-foreground">
                  <I18nText text="Loading..." />
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>

      {nextCursor && (
        <div className="flex justify-center">
          <Button variant="outline" size="sm" disabled={isLoading} onClick={() => setCursor(nextCursor ?? undefined)}>
            <I18nText text="Load more" />
          </Button>
        </div>
      )}

      <Dialog open={detail !== null} onOpenChange={(open) => !open && setDetail(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              <I18nText text="Delivery detail" />
            </DialogTitle>
            <DialogDescription>
              <I18nText text="Lifecycle, attempts, and the most recent delivery error for this record." />
            </DialogDescription>
          </DialogHeader>
          {detail && (
            <div className="flex flex-col gap-3 text-sm">
              <div className="grid grid-cols-3 gap-2">
                <span className="text-muted-foreground">Status</span>
                <span className="col-span-2">{statusLabel(detail.status)}</span>
                <span className="text-muted-foreground">Attempts</span>
                <span className="col-span-2">{detail.attempts}</span>
                <span className="text-muted-foreground">Generation</span>
                <span className="col-span-2">{detail.generation}</span>
                <span className="text-muted-foreground">Destination</span>
                <span className="col-span-2 break-all font-mono text-xs">{detail.destination}</span>
                <span className="text-muted-foreground">Event key</span>
                <span className="col-span-2 break-all font-mono text-xs">{detail.eventKey}</span>
                {detail.lastErrorCategory && (
                  <>
                    <span className="text-muted-foreground">Error category</span>
                    <span className="col-span-2">{categoryLabel(detail.lastErrorCategory)}</span>
                  </>
                )}
                {detail.lastErrorSummary && (
                  <>
                    <span className="text-muted-foreground">Error summary</span>
                    <span className="col-span-2 whitespace-pre-wrap break-words rounded bg-muted p-2 text-xs">
                      {detail.lastErrorSummary}
                    </span>
                  </>
                )}
                {detail.deadLetterReason && (
                  <>
                    <span className="text-muted-foreground">Dead-letter reason</span>
                    <span className="col-span-2">{detail.deadLetterReason}</span>
                  </>
                )}
                <span className="text-muted-foreground">Last attempt</span>
                <span className="col-span-2">{detail.lastAttemptAt ?? '-'}</span>
                <span className="text-muted-foreground">Next attempt</span>
                <span className="col-span-2">{detail.nextAttemptAt ?? '-'}</span>
                <span className="text-muted-foreground">Delivered at</span>
                <span className="col-span-2">{detail.deliveredAt ?? '-'}</span>
                <span className="text-muted-foreground">Dead-lettered at</span>
                <span className="col-span-2">{detail.deadLetterAt ?? '-'}</span>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}
