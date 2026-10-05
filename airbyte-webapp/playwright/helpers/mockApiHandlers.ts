export type MockResponse = { json: unknown; status?: number } | { status: number; json?: never };

export type ApiHandler = (body: Record<string, unknown>) => MockResponse | Promise<MockResponse>;

export function handleRequest<Request>(handle: (request: Request) => MockResponse | Promise<MockResponse>): ApiHandler {
  return (body) => handle(body as unknown as Request);
}
