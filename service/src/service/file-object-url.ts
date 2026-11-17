export function fileObjectMetadataUrl(baseUrl: string, sessionId: string, fileId: string): string {
  return `${baseUrl}/sessions/${encodeURIComponent(sessionId)}/objects/${encodeURIComponent(fileId)}/metadata`;
}
