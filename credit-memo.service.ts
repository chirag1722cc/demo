import { Injectable, NgZone } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, Subject } from 'rxjs';
import { environment } from '../../environments/environment';

export interface GenerateRequest {
  customerId: string;
  requestDetails: string;
}

export interface MemoEvent {
  jobId: string;
  eventType: string;        // ACK | COMPLETED | FAILED | STATUS
  uiMessage: string;
  status?: string;
  resultContent?: string;
  errorMessage?: string;
}

export interface JobStatusResponse {
  jobId: string;
  status: 'PENDING' | 'IN_PROGRESS' | 'COMPLETED' | 'FAILED';
  uiMessage: string;
  resultContent: string;
  errorMessage: string;
  updatedAt: string;
}

@Injectable({ providedIn: 'root' })
export class CreditMemoService {

  private readonly base = `${environment.apiUrl}/api/credit-memo`;

  constructor(private http: HttpClient, private zone: NgZone) {}

  // ── Step 1+2: POST /generate ──────────────────────────────────────────────
  generate(request: GenerateRequest): Promise<string> {
    return new Promise((resolve, reject) => {
      this.http.post<{ jobId: string }>(`${this.base}/generate`, request)
        .subscribe({ next: r => resolve(r.jobId), error: reject });
    });
  }

  // ── SSE + polling fallback ────────────────────────────────────────────────
  //
  // Primary:  SSE connection receives ACK (step 5) and RESPONSE (step 9) instantly
  // Fallback: If SSE drops, exponential backoff polling covers both events
  //
  connectAndListen(jobId: string): Observable<MemoEvent> {
    const subject = new Subject<MemoEvent>();
    let sseActive = false;
    let pollTimeout: any;
    let pollAttempt = 0;
    let cancelled = false;

    // ── SSE ────────────────────────────────────────────────────────────────
    const startSse = () => {
      const es = new EventSource(`${this.base}/events/${jobId}`);
      sseActive = true;

      // Initial status on connect
      es.addEventListener('STATUS', (e: any) => {
        const data = JSON.parse(e.data);
        this.zone.run(() => subject.next({ ...data, eventType: 'STATUS' }));
      });

      // Step 5: ucldd ACK received — UI shows "In Process"
      es.addEventListener('ACK', (e: any) => {
        const data = JSON.parse(e.data);
        this.zone.run(() => subject.next({ ...data, eventType: 'ACK' }));
      });

      // Step 9: LLM response received — UI shows "Completed"
      es.addEventListener('COMPLETED', (e: any) => {
        const data = JSON.parse(e.data);
        this.zone.run(() => {
          subject.next({ ...data, eventType: 'COMPLETED' });
          subject.complete();
        });
        es.close();
      });

      es.addEventListener('FAILED', (e: any) => {
        const data = JSON.parse(e.data);
        this.zone.run(() => {
          subject.next({ ...data, eventType: 'FAILED' });
          subject.complete();
        });
        es.close();
      });

      // SSE dropped — switch to polling fallback
      es.onerror = () => {
        es.close();
        sseActive = false;
        if (!cancelled) {
          console.warn(`SSE dropped for jobId=${jobId}, switching to polling`);
          schedulePoll();
        }
      };

      return es;
    };

    // ── Polling fallback ───────────────────────────────────────────────────
    // 5s → 10s → 20s → 30s (cap) — only runs if SSE is down
    const getDelay = () => Math.min(5000 * Math.pow(2, pollAttempt++), 30_000);

    const schedulePoll = () => {
      if (cancelled || sseActive) return;
      pollTimeout = setTimeout(poll, getDelay());
    };

    const poll = () => {
      if (cancelled || sseActive) return;

      this.http.get<JobStatusResponse>(`${this.base}/status/${jobId}`)
        .subscribe({
          next: (status) => {
            this.zone.run(() => subject.next({
              jobId:         status.jobId,
              eventType:     status.status,
              uiMessage:     status.uiMessage,
              resultContent: status.resultContent,
              errorMessage:  status.errorMessage
            }));

            if (status.status === 'COMPLETED' || status.status === 'FAILED') {
              subject.complete();
            } else {
              schedulePoll();
            }
          },
          error: () => schedulePoll() // network blip — keep retrying
        });
    };

    // Start SSE immediately
    const es = startSse();

    // Teardown
    return new Observable(observer => {
      const sub = subject.subscribe(observer);
      return () => {
        cancelled = true;
        clearTimeout(pollTimeout);
        es.close();
        sub.unsubscribe();
      };
    });
  }
}
