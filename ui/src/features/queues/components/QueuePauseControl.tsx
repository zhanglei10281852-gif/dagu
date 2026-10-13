// Copyright (C) 2026 Yota Hamada
// SPDX-License-Identifier: GPL-3.0-or-later

import { PauseCircle, PlayCircle } from 'lucide-react';
import React from 'react';
import { components } from '@/api/v1/schema';
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
import { useErrorModal } from '@/components/ui/error-modal';
import { useCanExecute } from '@/contexts/AuthContext';
import { AppBarContext } from '@/contexts/AppBarContext';
import { useClient } from '@/hooks/api';
import { I18nProps } from '@/i18n/I18nProps';
import { I18nText } from '@/i18n/I18nText';
import { useI18n } from '@/i18n/I18nProvider';

type QueuePause = components['schemas']['Queue']['pause'];

interface QueuePauseProps {
  queueName: string;
  pause?: QueuePause;
  onChanged: () => Promise<void> | void;
}

function formatPausedAt(value?: string): string {
  if (!value) {
    return '';
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString();
}

function useQueuePauseMutation({ queueName, onChanged }: QueuePauseProps) {
  const client = useClient();
  const appBarContext = React.useContext(AppBarContext);
  const remoteNode = appBarContext.selectedRemoteNode || 'local';

  return React.useCallback(
    async (paused: boolean, reason?: string) => {
      const { error } = await client.POST('/queues/{name}/pause', {
        params: {
          path: { name: queueName },
          query: { remoteNode },
        },
        body: { paused, reason },
      });
      if (error) {
        throw new Error(error.message || 'Failed to update queue pause state');
      }
      await onChanged();
    },
    [client, queueName, remoteNode, onChanged]
  );
}

export function QueuePauseButton({ queueName, pause, onChanged }: QueuePauseProps) {
  const { ts } = useI18n();
  const setPaused = useQueuePauseMutation({ queueName, pause, onChanged });
  const { showError } = useErrorModal();
  const canExecute = useCanExecute();

  const paused = pause?.paused === true;
  const [dialogOpen, setDialogOpen] = React.useState(false);
  const [reason, setReason] = React.useState('');
  const [submitting, setSubmitting] = React.useState(false);

  const openPauseDialog = React.useCallback(() => {
    setReason(pause?.reason ?? '');
    setDialogOpen(true);
  }, [pause?.reason]);

  const handleConfirmPause = React.useCallback(async () => {
    setSubmitting(true);
    try {
      await setPaused(true, reason.trim() ? reason : undefined);
      setDialogOpen(false);
    } catch (err) {
      showError(
        err instanceof Error ? err.message : ts('Failed to pause the queue'),
        ts('Queued runs are kept. Please try again.')
      );
    } finally {
      setSubmitting(false);
    }
  }, [reason, setPaused, showError, ts]);

  const handleResume = React.useCallback(async () => {
    setSubmitting(true);
    try {
      await setPaused(false);
    } catch (err) {
      showError(
        err instanceof Error ? err.message : ts('Failed to resume the queue'),
        ts('Please try again or check the server connection.')
      );
    } finally {
      setSubmitting(false);
    }
  }, [setPaused, showError, ts]);

  if (!canExecute) {
    return null;
  }

  return (
    <>
      <Button
        size="sm"
        variant={paused ? 'default' : 'outline'}
        onClick={paused ? handleResume : openPauseDialog}
        disabled={submitting}
        aria-label={paused ? 'Resume queue' : 'Pause queue'}
      >
        {paused ? (
          <PlayCircle className="h-3.5 w-3.5 text-success" />
        ) : (
          <PauseCircle className="h-3.5 w-3.5" />
        )}
        <span className="ml-1">
          {paused ? <I18nText text={'Resume'} /> : <I18nText text={'Pause'} />}
        </span>
      </Button>

      <I18nProps>
        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogContent className="sm:max-w-lg">
            <DialogHeader>
              <DialogTitle>
                <I18nText text={'Pause queue'} />
              </DialogTitle>
              <DialogDescription>
                <I18nText
                  text={
                    'Pausing stops new local and distributed runs for this queue only. Running runs finish normally, and queued runs are kept in order until resume.'
                  }
                />
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-2">
              <label
                htmlFor="queue-pause-reason"
                className="text-xs font-medium text-muted-foreground"
              >
                <I18nText text={'Reason (optional)'} />
              </label>
              <Textarea
                id="queue-pause-reason"
                value={reason}
                maxLength={512}
                onChange={(event) => setReason(event.target.value)}
                placeholder={ts('e.g. Downstream system under maintenance')}
                className="min-h-20 text-sm"
              />
            </div>
            <DialogFooter>
              <Button
                variant="outline"
                onClick={() => setDialogOpen(false)}
                disabled={submitting}
              >
                <I18nText text={'Cancel'} />
              </Button>
              <Button onClick={handleConfirmPause} disabled={submitting}>
                <I18nText text={'Pause queue'} />
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </I18nProps>
    </>
  );
}

export function QueuePauseBanner({ pause }: { pause?: QueuePause }) {
  const { ts } = useI18n();
  if (pause?.paused !== true) {
    return null;
  }
  const detail = [
    pause.pausedBy ? ts('paused by {user}', { user: pause.pausedBy }) : '',
    formatPausedAt(pause.pausedAt),
    pause.reason,
  ]
    .filter(Boolean)
    .join(' — ');

  return (
    <div
      role="status"
      className="flex flex-wrap items-center gap-x-2 gap-y-1 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-sm text-warning"
    >
      <PauseCircle className="h-4 w-4 shrink-0" />
      <span className="font-medium">
        <I18nText
          text={'Queue paused. No new runs start; queued runs are kept in order.'}
        />
      </span>
      {detail && <span className="text-xs text-warning/90">{detail}</span>}
    </div>
  );
}
