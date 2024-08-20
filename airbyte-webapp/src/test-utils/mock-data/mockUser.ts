import { UserRead } from "core/api/types/AirbyteClient";

export const mockUser: UserRead = {
  userId: "mock-user",
  email: "mockUser@airbyte.io",
  authProvider: "airbyte",
  authUserId: "mock-user",
  metadata: {},
};
