import { describe, expect, test } from 'bun:test';
import { decodeContinuationToken } from './programmatic-continuation-token';

function token(execution_id: string, ts = Date.now()): string {
  return Buffer.from(JSON.stringify({ execution_id, ts })).toString('base64');
}

describe('decodeContinuationToken', () => {
  test('accepts a generated execution ID', () => {
    expect(
      decodeContinuationToken(token('aBc123_-defGhi456jKlM'), 600_000),
    ).toEqual({ execution_id: 'aBc123_-defGhi456jKlM' });
  });

  test.each([
    '../internal',
    'https://internal.example',
    'aaaaaaaaaaaaaaaaaaaa/',
    'short',
  ])('rejects an unsafe execution ID: %s', execution_id => {
    expect(decodeContinuationToken(token(execution_id), 600_000)).toBeNull();
  });

  test('rejects expired continuation tokens', () => {
    expect(
      decodeContinuationToken(
        token('aBc123_-defGhi456jKlM', Date.now() - 2_000),
        1_000,
      ),
    ).toBeNull();
  });
});
