import { DestinationOperation } from "core/api/types/AirbyteClient";

export const MOCK_OPERATION_NO_REQUIRED_FIELD: DestinationOperation = {
  objectName: "Event",
  syncMode: "append",
  schema: {
    additionalProperties: false,
    type: "object",
    properties: {
      Name: {
        type: "string",
      },
      Id: {
        type: "string",
      },
    },
  },
  matchingKeys: [],
};

export const MOCK_OPERATION_WITH_REQUIRED_FIELD: DestinationOperation = {
  objectName: "clazar__Buyer__Share",
  syncMode: "append",
  schema: {
    additionalProperties: false,
    type: "object",
    required: ["ParentId", "UserOrGroupId", "AccessLevel"],
    properties: {
      ParentId: {
        type: "string",
      },
      UserOrGroupId: {
        type: "string",
      },
      RowCause: {
        type: ["null", "string"],
      },
      AccessLevel: {
        type: "string",
      },
    },
  },
  matchingKeys: [],
};

export const ALL_MOCK_OPERATIONS: DestinationOperation[] = [
  MOCK_OPERATION_NO_REQUIRED_FIELD,
  MOCK_OPERATION_WITH_REQUIRED_FIELD,
];
