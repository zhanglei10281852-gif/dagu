// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

import React from 'react';
import { Lock, LockOpen, ShieldCheck } from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Textarea } from '@/components/ui/textarea';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip';
import { useErrorModal } from '@/components/ui/error-modal';
import { useSimpleToast } from '@/components/ui/simple-toast';
import { useCanWrite } from '@/contexts/AuthContext';
import { useRemoteNode } from '@/contexts/RemoteNodeContext';
import { useClient } from '@/hooks/api';
import dayjs from '@/lib/dayjs';
import { components, Status } from '@/api/v1/schema';
import { I18nText } from '@/i18n/I18nText';
import { useI18n } from '@/i18n/I18nProvider';

type DAGRunDetails = components['schemas']['DAGRunDetails'];

type Props = {
  dagRun: DAGRunDetails;
  refresh?: () => void;
};

const terminalStatuses = new Set<Status>([
  Status.Failed,
  Status.Aborted,
  Status.Success,
  Status.PartialSuccess,
  Status.Rejected,
]);

/**
 * DAGRunRetentionControl places and releases the evidence retention hold on a
 * finished root DAG-run. While held, all cleanup and deletion paths skip the
 * run. The hold metadata (operator, time, optional reason) is shown in a
 * tooltip. Users without DAG write permission see the badge only.
 */
function DAGRunRetentionControl({ dagRun, refresh }: Props) {
  const client = useClient();
  const remoteNode = useRemoteNode();
  const canWrite = useCanWrite();
  const { showError } = useErrorModal();
  const { showToast } = useSimpleToast();
  const { ts } = useI18n();

  const [retainOpen, setRetainOpen] = React.useState(false);
  const [releaseOpen, setReleaseOpen] = React.useState(false);
  const [reason, setReason] = React.useState('');
  const [submitting, setSubmitting] = React.useState(false);

  const isRoot = dagRun.rootDAGRunId === dagRun.dagRunId;
  const isTerminal = terminalStatuses.has(dagRun.status);
  const retention = dagRun.retention;

  if (!isRoot || !isTerminal) {
    return null;
  }

  const pathParams = {
    name: dagRun.name,
    dagRunId: dagRun.dagRunId,
  };

  const submitRetain = async () => {
    setSubmitting(true);
    const trimmedReason = reason.trim();
    const { error } = await client.PUT(
      '/dag-runs/{name}/{dagRunId}/retention',
      {
        params: {
          path: pathParams,
          query: { remoteNode },
        },
        body: trimmedReason ? { reason: trimmedReason } : {},
      }
    );
    setSubmitting(false);
    if (error) {
      showError(
        error.message || ts('Failed to place the retention hold'),
        ts(
          'Only finished root DAG-runs can be retained. Check the server logs for details.'
        )
      );
      return;
    }
    setRetainOpen(false);
    setReason('');
    showToast(ts('DAG run retained as evidence'));
    refresh?.();
  };

  const submitRelease = async () => {
    setSubmitting(true);
    const { error } = await client.DELETE(
      '/dag-runs/{name}/{dagRunId}/retention',
      {
        params: {
          path: pathParams,
          query: { remoteNode },
        },
      }
    );
    setSubmitting(false);
    if (error) {
      showError(
        error.message || ts('Failed to release the retention hold'),
        ts('The DAG run remains retained as evidence.')
      );
      return;
    }
    setReleaseOpen(false);
    showToast(ts('Retention hold released'));
    refresh?.();
  };

  const retentionDetails = retention ? (
    <div className="space-y-1">
      {retention.lockedBy && (
        <div>
          {ts('Retained by {name}', { name: retention.lockedBy })}
        </div>
      )}
      <div>{dayjs(retention.lockedAt).format('YYYY-MM-DD HH:mm:ss Z')}</div>
      {retention.reason && (
        <div className="max-w-xs whitespace-pre-wrap break-words text-xs opacity-90">
          {retention.reason}
        </div>
      )}
    </div>
  ) : null;

  return (
    <TooltipProvider delayDuration={300}>
      {retention ? (
        <div className="flex items-center gap-1">
          <Tooltip>
            <TooltipTrigger asChild>
              <span className="inline-flex items-center gap-1.5 rounded-md border border-amber-500/40 bg-amber-500/10 px-2 py-1 text-xs font-medium text-amber-700 dark:text-amber-300">
                <Lock className="h-3.5 w-3.5" />
                <I18nText text={'Retained'} />
              </span>
            </TooltipTrigger>
            <TooltipContent>{retentionDetails}</TooltipContent>
          </Tooltip>
          {canWrite && (
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="h-7 px-2 text-xs"
              onClick={() => setReleaseOpen(true)}
            >
              <LockOpen className="h-3.5 w-3.5" />
              <I18nText text={'Release retention'} />
            </Button>
          )}
        </div>
      ) : (
        canWrite && (
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-7 px-2 text-xs"
            onClick={() => {
              setReason('');
              setRetainOpen(true);
            }}
          >
            <ShieldCheck className="h-3.5 w-3.5" />
            <I18nText text={'Retain as evidence'} />
          </Button>
        )
      )}

      <Dialog
        open={retainOpen}
        onOpenChange={(open) => {
          if (!submitting) {
            setRetainOpen(open);
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              <I18nText text={'Retain DAG run as evidence'} />
            </DialogTitle>
            <DialogDescription>
              <I18nText
                text={
                  'The run, its attempts, child runs, logs, artifacts, and working directory will be exempt from history cleanup and deletion until the hold is released.'
                }
              />
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2">
            <label
              htmlFor="retention-reason"
              className="text-sm font-medium"
            >
              <I18nText text={'Reason (optional)'} />
            </label>
            <Textarea
              id="retention-reason"
              maxLength={1000}
              rows={4}
              value={reason}
              onChange={(event) => setReason(event.target.value)}
              placeholder={ts(
                'For example: incident investigation, audit hold'
              )}
            />
          </div>
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => setRetainOpen(false)}
              disabled={submitting}
            >
              <I18nText text={'Cancel'} />
            </Button>
            <Button
              type="button"
              onClick={submitRetain}
              disabled={submitting}
            >
              <I18nText text={'Retain run'} />
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={releaseOpen}
        onOpenChange={(open) => {
          if (!submitting) {
            setReleaseOpen(open);
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              <I18nText text={'Release retention hold'} />
            </DialogTitle>
            <DialogDescription>
              <I18nText
                text={
                  'The DAG run will follow the normal history retention policy again and may be removed by cleanup.'
                }
              />
            </DialogDescription>
          </DialogHeader>
          {retentionDetails && (
            <div className="rounded-md border bg-muted/30 p-3 text-sm">
              {retentionDetails}
            </div>
          )}
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => setReleaseOpen(false)}
              disabled={submitting}
            >
              <I18nText text={'Cancel'} />
            </Button>
            <Button
              type="button"
              variant="destructive"
              onClick={submitRelease}
              disabled={submitting}
            >
              <I18nText text={'Release hold'} />
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </TooltipProvider>
  );
}

export default DAGRunRetentionControl;
