import { Component, OnDestroy } from '@angular/core';
import { Subscription } from 'rxjs';
import { CreditMemoService, MemoEvent } from '../../services/credit-memo.service';

// UI states map directly to the 9-step use case
type ViewState =
  | 'idle'          // Step 1  — form shown
  | 'submitting'    // Step 2  — POST in flight
  | 'waiting_ack'   // Step 3  — request sent, waiting for ucldd ACK
  | 'in_progress'   // Step 5  — ACK received, LLM processing
  | 'completed'     // Step 9  — memo ready
  | 'failed';       //          — error at any step

@Component({
  selector: 'app-credit-memo',
  templateUrl: './credit-memo.component.html',
  styleUrls: ['./credit-memo.component.scss']
})
export class CreditMemoComponent implements OnDestroy {

  // Form inputs
  customerId = '';
  requestDetails = '';

  // State
  viewState: ViewState = 'idle';
  jobId: string | null = null;
  uiMessage = '';
  resultContent: string | null = null;
  errorMessage: string | null = null;

  private eventSub?: Subscription;

  constructor(private creditMemoService: CreditMemoService) {}

  // ── Step 1: User clicks "Start New Memo" ─────────────────────────────────
  async onStartNewMemo() {
    if (!this.customerId.trim() || !this.requestDetails.trim()) return;

    this.viewState = 'submitting';
    this.uiMessage = 'Submitting request...';
    this.errorMessage = null;
    this.resultContent = null;

    try {
      // Step 2: POST /generate → returns jobId
      this.jobId = await this.creditMemoService.generate({
        customerId:     this.customerId,
        requestDetails: this.requestDetails
      });

      // Open SSE connection immediately after getting jobId
      // Waiting for ACK from ucldd (step 3/4)
      this.viewState = 'waiting_ack';
      this.uiMessage = 'Request submitted. Waiting for confirmation...';
      this.listenForEvents(this.jobId);

    } catch {
      this.viewState = 'failed';
      this.uiMessage = '';
      this.errorMessage = 'Failed to submit request. Please try again.';
    }
  }

  // ── Listen for SSE events (with polling fallback) ─────────────────────────
  private listenForEvents(jobId: string) {
    this.eventSub = this.creditMemoService.connectAndListen(jobId).subscribe({
      next: (event: MemoEvent) => this.handleEvent(event),
      error: () => {
        this.viewState = 'failed';
        this.errorMessage = 'Connection lost. Please refresh and check memo status.';
      }
    });
  }

  // ── Handle each event from SSE / polling ──────────────────────────────────
  private handleEvent(event: MemoEvent) {
    switch (event.eventType) {

      case 'STATUS':
        // Initial status push on SSE connect — sync UI to current DB state
        this.syncToStatus(event.status || '', event);
        break;

      case 'ACK':
        // Step 5: ucldd confirmed receipt — LLM is now processing
        this.viewState = 'in_progress';
        this.uiMessage = event.uiMessage || 'Request received. Generating credit memo...';
        break;

      case 'IN_PROGRESS':
        // From polling fallback
        this.viewState = 'in_progress';
        this.uiMessage = event.uiMessage || 'Generating credit memo...';
        break;

      case 'COMPLETED':
        // Step 9: memo ready — flip UI from in-process to completed
        this.viewState = 'completed';
        this.uiMessage = event.uiMessage || 'Credit memo generated successfully.';
        this.resultContent = event.resultContent || null;
        break;

      case 'FAILED':
        this.viewState = 'failed';
        this.errorMessage = event.errorMessage || 'Generation failed. Please try again.';
        break;
    }
  }

  private syncToStatus(status: string, event: MemoEvent) {
    switch (status) {
      case 'PENDING':
        this.viewState = 'waiting_ack';
        this.uiMessage = 'Waiting for confirmation...';
        break;
      case 'IN_PROGRESS':
        this.viewState = 'in_progress';
        this.uiMessage = event.uiMessage || 'Generating credit memo...';
        break;
      case 'COMPLETED':
        this.viewState = 'completed';
        this.resultContent = event.resultContent || null;
        break;
      case 'FAILED':
        this.viewState = 'failed';
        this.errorMessage = event.errorMessage || 'Generation failed.';
        break;
    }
  }

  // ── Reset ─────────────────────────────────────────────────────────────────
  onReset() {
    this.eventSub?.unsubscribe();
    this.viewState = 'idle';
    this.jobId = null;
    this.uiMessage = '';
    this.resultContent = null;
    this.errorMessage = null;
    this.customerId = '';
    this.requestDetails = '';
  }

  copyResult() {
    if (this.resultContent) {
      navigator.clipboard.writeText(this.resultContent);
    }
  }

  ngOnDestroy() {
    this.eventSub?.unsubscribe();
  }
}
