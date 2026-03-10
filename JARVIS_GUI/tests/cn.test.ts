import { describe, expect, it } from 'vitest';
import { cn } from '@/lib/utils';

describe('cn utility', () => {
  it('merges class names and resolves tailwind conflicts', () => {
    const result = cn('p-2', 'p-4', 'text-sm', false && 'hidden');
    expect(result).toContain('p-4');
    expect(result).toContain('text-sm');
    expect(result).not.toContain('p-2');
  });
});

