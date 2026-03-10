import { describe, expect, it } from 'vitest';
import { ARG_TEMPLATES, QUICK_ACTIONS } from '@/components/os/action-templates';

describe('action templates', () => {
  it('includes external connector action templates', () => {
    const requiredActions = [
      'external_connector_status',
      'external_email_send',
      'external_email_list',
      'external_email_read',
      'external_calendar_create_event',
      'external_calendar_list_events',
      'external_calendar_update_event',
      'external_doc_create',
      'external_doc_list',
      'external_doc_read',
      'external_doc_update',
      'external_task_list',
      'external_task_create',
      'external_task_update',
    ];
    for (const action of requiredActions) {
      expect(ARG_TEMPLATES[action]).toBeDefined();
    }
  });

  it('uses explicit identifier placeholders for read/update actions', () => {
    expect(ARG_TEMPLATES.external_email_read?.message_id).toBe('MESSAGE_ID_HERE');
    expect(ARG_TEMPLATES.external_calendar_update_event?.event_id).toBe('EVENT_ID_HERE');
    expect(ARG_TEMPLATES.external_doc_read?.document_id).toBe('DOCUMENT_ID_HERE');
    expect(ARG_TEMPLATES.external_doc_update?.document_id).toBe('DOCUMENT_ID_HERE');
    expect(ARG_TEMPLATES.external_task_update?.task_id).toBe('TASK_ID_HERE');
  });

  it('exposes connector health as a quick action', () => {
    expect(QUICK_ACTIONS).toContain('external_connector_status');
    expect(QUICK_ACTIONS).toContain('external_task_list');
  });
});
