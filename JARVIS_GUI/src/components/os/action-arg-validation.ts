const TASK_PROVIDER_VALUES = new Set([
  'auto',
  'google',
  'gtasks',
  'google_tasks',
  'graph',
  'graph_todo',
  'microsoft',
  'microsoft_graph',
  'microsoft_todo',
  'todo',
]);

const TASK_STATUS_VALUES = new Set([
  'done',
  'complete',
  'completed',
  'closed',
  'finished',
  'resolved',
  'todo',
  'open',
  'pending',
  'active',
  'in_progress',
  'notstarted',
  'not_started',
  'needsaction',
  'needsAction',
  'notStarted',
]);

const CLICK_TARGET_MODE_VALUES = new Set(['auto', 'accessibility', 'ocr']);
const CLICK_VERIFY_MODE_VALUES = new Set(['state_or_visibility', 'hash_changed', 'visible', 'none']);
const CLICK_BUTTON_VALUES = new Set(['left', 'right', 'middle']);

function normalizeText(value: unknown): string {
  return String(value ?? '').trim();
}

function normalizeLower(value: unknown): string {
  return normalizeText(value).toLowerCase();
}

function hasTextField(args: Record<string, unknown>, field: string): boolean {
  return normalizeText(args[field]).length > 0;
}

function isLikelyDueValue(raw: string): boolean {
  if (!raw) return true;
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return true;
  if (!/^\d{4}-\d{2}-\d{2}[T ]/.test(raw)) return false;
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed);
}

export function validateActionArgs(action: string, args: Record<string, unknown>): string[] {
  const errors: string[] = [];
  const normalizedAction = normalizeLower(action);

  if (normalizedAction === 'computer_click_target') {
    if (!hasTextField(args, 'query')) {
      errors.push('query must be a non-empty string.');
    }

    const targetMode = normalizeLower(args.target_mode);
    if (targetMode && !CLICK_TARGET_MODE_VALUES.has(targetMode)) {
      errors.push('target_mode must be one of: auto, accessibility, ocr.');
    }

    const verifyMode = normalizeLower(args.verify_mode);
    if (verifyMode && !CLICK_VERIFY_MODE_VALUES.has(verifyMode)) {
      errors.push('verify_mode must be one of: state_or_visibility, hash_changed, visible, none.');
    }

    const button = normalizeLower(args.button);
    if (button && !CLICK_BUTTON_VALUES.has(button)) {
      errors.push('button must be one of: left, right, middle.');
    }
  }

  if (normalizedAction === 'external_task_list') {
    const provider = normalizeLower(args.provider);
    if (provider && !TASK_PROVIDER_VALUES.has(provider)) {
      errors.push('provider is not recognized for tasks (use auto/google/graph variants).');
    }

    if (args.max_results !== undefined) {
      const parsed = Number(args.max_results);
      if (!Number.isFinite(parsed) || parsed < 1 || parsed > 200) {
        errors.push('max_results must be a number between 1 and 200.');
      }
    }
  }

  if (normalizedAction === 'external_task_create' || normalizedAction === 'external_task_update') {
    const provider = normalizeLower(args.provider);
    if (provider && !TASK_PROVIDER_VALUES.has(provider)) {
      errors.push('provider is not recognized for tasks (use auto/google/graph variants).');
    }

    const status = normalizeText(args.status);
    if (status && !TASK_STATUS_VALUES.has(status)) {
      errors.push('status is not recognized for tasks.');
    }

    const due = normalizeText(args.due || args.due_at);
    if (due && !isLikelyDueValue(due)) {
      errors.push('due or due_at must be ISO date/date-time (e.g., 2026-03-05 or 2026-03-05T17:00:00Z).');
    }
  }

  if (normalizedAction === 'external_task_create') {
    if (!hasTextField(args, 'title')) {
      errors.push('title must be a non-empty string.');
    }
  }

  if (normalizedAction === 'external_task_update') {
    if (!hasTextField(args, 'task_id')) {
      errors.push('task_id must be a non-empty string.');
    }
    const hasMutableField =
      hasTextField(args, 'title') ||
      hasTextField(args, 'notes') ||
      hasTextField(args, 'due') ||
      hasTextField(args, 'due_at') ||
      hasTextField(args, 'status');
    if (!hasMutableField) {
      errors.push('Provide at least one mutable field: title, notes, due/due_at, or status.');
    }
  }

  return errors;
}
