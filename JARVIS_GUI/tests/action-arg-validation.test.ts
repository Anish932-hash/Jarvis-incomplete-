import { describe, expect, it } from 'vitest';
import { validateActionArgs } from '@/components/os/action-arg-validation';

describe('action arg validation', () => {
  it('accepts valid computer click target args', () => {
    const errors = validateActionArgs('computer_click_target', {
      query: 'Submit',
      target_mode: 'auto',
      verify_mode: 'state_or_visibility',
      button: 'left',
    });
    expect(errors).toEqual([]);
  });

  it('rejects invalid click target mode values', () => {
    const errors = validateActionArgs('computer_click_target', {
      query: 'Submit',
      target_mode: 'vision',
      verify_mode: 'hash_then_visible',
    });
    expect(errors.some((item) => item.includes('target_mode'))).toBe(true);
    expect(errors.some((item) => item.includes('verify_mode'))).toBe(true);
  });

  it('rejects invalid task create due/status values', () => {
    const errors = validateActionArgs('external_task_create', {
      provider: 'google',
      title: 'Review release notes',
      due: 'next friday',
      status: 'paused',
    });
    expect(errors.some((item) => item.includes('status'))).toBe(true);
    expect(errors.some((item) => item.includes('due'))).toBe(true);
  });

  it('rejects task update without mutable fields', () => {
    const errors = validateActionArgs('external_task_update', {
      provider: 'graph',
      task_id: 'task-42',
    });
    expect(errors.some((item) => item.includes('mutable field'))).toBe(true);
  });

  it('rejects unknown task provider alias', () => {
    const errors = validateActionArgs('external_task_list', {
      provider: 'jira',
      max_results: 20,
    });
    expect(errors.some((item) => item.includes('provider'))).toBe(true);
  });
});
