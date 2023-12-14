type ErrorDetails = ErrorWithMessage | ErrorWithDetail;
interface ErrorWithMessage {
  // Why `string | undefined` instead of an optional field? We may not always be able to
  // dynamically find our intended message, but we should still statically verify that
  // we're at least *attempting* to set either a message or a detail object by requiring
  // one of the property names to exist.
  message: string | undefined;
  detail?: unknown;
}
interface ErrorWithDetail {
  detail: unknown;
  message?: string;
}

export class CommonRequestError<ErrorPayload extends ErrorDetails = ErrorDetails> extends Error {
  __type = "common.error";
  // TODO: Add better error hierarchy
  _status?: number;
  payload?: ErrorPayload;

  constructor(
    protected response: Response | undefined,
    payload?: ErrorPayload
  ) {
    super(response?.statusText);
    this.response = response;
    this.message = payload?.message ?? JSON.stringify(payload?.detail) ?? "common.error";
    this.payload = payload;
  }

  get status() {
    return this._status || this.response?.status;
  }
}

export function isCommonRequestError(error: { __type?: string }): error is CommonRequestError {
  return error.__type === "common.error";
}
