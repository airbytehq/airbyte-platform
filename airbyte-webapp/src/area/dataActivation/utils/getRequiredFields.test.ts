import { getRequiredFields } from "./getRequiredFields";
import {
  MOCK_OPERATION_NO_REQUIRED_FIELD,
  MOCK_OPERATION_WITH_REQUIRED_FIELD,
} from "../../../test-utils/mock-data/mockDestinationOperations";

describe(`${getRequiredFields.name}`, () => {
  it("returns an empty array when schema.required is not a string array", () => {
    expect(getRequiredFields(MOCK_OPERATION_NO_REQUIRED_FIELD)).toEqual([]);
  });
  it("returns an array of required fields when the schema has them", () => {
    expect(getRequiredFields(MOCK_OPERATION_WITH_REQUIRED_FIELD)).toEqual(["ParentId", "UserOrGroupId", "AccessLevel"]);
  });
});
