import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";
import en from "locales/en.json";

import { DataActivationConnectionFormSchema } from "./DataActivationConnectionFormSchema";

const DUPLICATE_ERROR_MESSAGE_ID = "connection.dataActivation.duplicateDestinationFieldName";
const REQUIRED_ERROR_MESSAGE_ID = "form.empty.error";

const buildField = (overrides = {}) => ({
  sourceFieldName: "source_field",
  destinationFieldName: "destination_field",
  additionalMappers: [],
  ...overrides,
});

const buildStream = (overrides = {}) => ({
  sourceStreamDescriptor: { name: "users" },
  destinationObjectName: "contacts",
  sourceSyncMode: SyncMode.full_refresh,
  cursorField: null,
  destinationSyncMode: DestinationSyncMode.append,
  matchingKeys: null,
  fields: [buildField()],
  ...overrides,
});

const parseStream = (overrides = {}) =>
  DataActivationConnectionFormSchema.safeParse({ streams: [buildStream(overrides)] });

const failureWith = (issue: { message: string; path: Array<string | number> }) =>
  expect.objectContaining({
    success: false,
    error: expect.objectContaining({ issues: expect.arrayContaining([expect.objectContaining(issue)]) }),
  });

describe("DataActivationConnectionFormSchema", () => {
  it("should accept a complete stream that does not need a cursor or matching keys", () => {
    expect(parseStream().success).toBe(true);
  });

  it("should reject a source stream name made only of whitespace", () => {
    expect(parseStream({ sourceStreamDescriptor: { name: "   " } })).toEqual(
      failureWith({ message: REQUIRED_ERROR_MESSAGE_ID, path: ["streams", 0, "sourceStreamDescriptor"] })
    );
  });

  it("should reject a destination object name made only of whitespace", () => {
    expect(parseStream({ destinationObjectName: "   " })).toEqual(
      failureWith({ message: REQUIRED_ERROR_MESSAGE_ID, path: ["streams", 0, "destinationObjectName"] })
    );
  });

  it("should require a source sync mode to be selected", () => {
    expect(parseStream({ sourceSyncMode: null })).toEqual(
      failureWith({ message: REQUIRED_ERROR_MESSAGE_ID, path: ["streams", 0, "sourceSyncMode"] })
    );
  });

  it("should require a cursor field for incremental sync", () => {
    expect(parseStream({ sourceSyncMode: SyncMode.incremental, cursorField: "" })).toEqual(
      failureWith({ message: REQUIRED_ERROR_MESSAGE_ID, path: ["streams", 0, "cursorField"] })
    );
  });

  it("should require a destination sync mode to be selected", () => {
    expect(parseStream({ destinationSyncMode: null, matchingKeys: null })).toEqual(
      failureWith({ message: REQUIRED_ERROR_MESSAGE_ID, path: ["streams", 0, "destinationSyncMode"] })
    );
  });

  it("should require at least one matching key when the destination expects matching keys", () => {
    expect(parseStream({ destinationSyncMode: DestinationSyncMode.append_dedup, matchingKeys: [] })).toEqual(
      failureWith({ message: REQUIRED_ERROR_MESSAGE_ID, path: ["streams", 0, "matchingKeys"] })
    );
  });

  it("should report a duplicate destination field on the repeated field", () => {
    const fields = ["email", "name", "email"].map((destinationFieldName) => buildField({ destinationFieldName }));

    expect(parseStream({ fields })).toEqual(
      expect.objectContaining({
        success: false,
        error: expect.objectContaining({
          issues: [
            expect.objectContaining({
              message: DUPLICATE_ERROR_MESSAGE_ID,
              path: ["streams", 0, "fields", 2, "destinationFieldName"],
            }),
          ],
        }),
      })
    );
  });

  it("should have a translation for the duplicate destination field message id", () => {
    expect(Object.keys(en)).toContain(DUPLICATE_ERROR_MESSAGE_ID);
  });

  it("should accept unique destination field names", () => {
    const fields = ["email", "name"].map((destinationFieldName) => buildField({ destinationFieldName }));

    expect(parseStream({ fields }).success).toBe(true);
  });

  it("should allow the same destination field name in different streams", () => {
    const result = DataActivationConnectionFormSchema.safeParse({
      streams: [buildStream(), buildStream({ sourceStreamDescriptor: { name: "orders" } })],
    });

    expect(result.success).toBe(true);
  });
});
