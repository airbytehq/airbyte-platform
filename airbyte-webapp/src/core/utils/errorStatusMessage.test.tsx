import { MessageDescriptor } from "react-intl";

import { FailureReason } from "core/api/types/AirbyteClient";

import { generateMessageFromError, FormError, getFailureType, failureUiDetailsFromReason } from "./errorStatusMessage";

describe("#generateMessageFromError", () => {
  const formatMessage = () => "";

  it("should return a provided error message", () => {
    const errMsg = "test";
    expect(generateMessageFromError(new Error(errMsg), formatMessage)).toBe(errMsg);
  });

  it("should return null if no error message and no status, or status is 0", () => {
    expect(generateMessageFromError(new Error(), formatMessage)).toBe(null);
    const fakeStatusError = new FormError();
    fakeStatusError.status = 0;
    expect(generateMessageFromError(fakeStatusError, formatMessage)).toBe(null);
  });

  it("should return a validation error message if status is 400", () => {
    const fakeStatusError = new FormError();
    fakeStatusError.status = 400;
    expect(generateMessageFromError(fakeStatusError, formatMessage)).toMatchInlineSnapshot(`
      <Memo(MemoizedFormattedMessage)
        id="form.validationError"
      />
    `);
  });

  it("should return a 'some error' message if status is > 0 and not 400", () => {
    const fakeStatusError = new FormError();
    fakeStatusError.status = 401;
    expect(generateMessageFromError(fakeStatusError, formatMessage)).toMatchInlineSnapshot(`
      <Memo(MemoizedFormattedMessage)
        id="form.someError"
      />
    `);
  });
});

describe("#getFailureType", () => {
  it("should return 'error' if failure type is 'config_error' and origin is 'source' or 'destination'", () => {
    expect(
      getFailureType({
        failureType: "config_error",
        failureOrigin: "source",
        timestamp: 0,
      })
    ).toBe("error");
    expect(
      getFailureType({
        failureType: "config_error",
        failureOrigin: "destination",
        timestamp: 0,
      })
    ).toBe("error");
  });

  it("should return 'warning' if failure type is 'config_error' and origin is not 'source' or 'destination'", () => {
    expect(getFailureType({ failureType: "config_error", failureOrigin: "airbyte_platform", timestamp: 0 })).toBe(
      "warning"
    );
  });

  it("should return 'warning' if failure type is not 'config_error'", () => {
    expect(getFailureType({ failureType: "system_error", failureOrigin: "source", timestamp: 0 })).toBe("warning");
  });
});

describe("#failureUiDetailsFromReason", () => {
  const formatMessage = (descriptor: MessageDescriptor) => {
    return descriptor.id ?? "";
  };

  it("should return correct UI details for a known error reason", () => {
    const reason: FailureReason = {
      failureOrigin: "source",
      externalMessage: "externalMessage",
      internalMessage: "internalMessage",
      failureType: "config_error",
      timestamp: 0,
    };
    const result = failureUiDetailsFromReason(reason, formatMessage);
    expect(result).toEqual({
      type: "error",
      typeLabel: "failureMessage.type.error",
      origin: "source",
      message: "externalMessage",
      secondaryMessage: undefined,
    });
  });

  it("should return correct UI details for a warning reason", () => {
    const reason: FailureReason = {
      failureOrigin: "airbyte_platform",
      failureType: "system_error",
      externalMessage: "externalMessage",
      internalMessage: "internalMessage",
      timestamp: 0,
    };
    const result = failureUiDetailsFromReason(reason, formatMessage);
    expect(result).toEqual({
      type: "warning",
      typeLabel: "failureMessage.type.warning",
      origin: "airbyte_platform",
      message: "externalMessage",
      secondaryMessage: "internalMessage",
    });
  });
});
