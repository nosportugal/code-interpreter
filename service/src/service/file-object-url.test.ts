import { describe, expect, test } from 'bun:test';
import { fileObjectMetadataUrl } from './file-object-url';

describe('fileObjectMetadataUrl', () => {
  test('preserves valid identifiers as path segments', () => {
    expect(fileObjectMetadataUrl('http://file-server', 'aBc123_-defGhi456jKlM', 'Abc123_-defGhi456jKlM'))
      .toBe('http://file-server/sessions/aBc123_-defGhi456jKlM/objects/Abc123_-defGhi456jKlM/metadata');
  });

  test('encodes path delimiters in identifiers', () => {
    expect(fileObjectMetadataUrl('http://file-server', '../internal', '../secret'))
      .toBe('http://file-server/sessions/..%2Finternal/objects/..%2Fsecret/metadata');
  });
});
