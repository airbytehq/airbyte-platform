import { v4 as uuidV4 } from "uuid";

import { UserRead } from "core/api/types/AirbyteClient";

import { createLDContext, createMultiContext, createUserContext, getSingleContextsFromMulti } from "./contexts";

const mockLocale = "en";

describe(`${createUserContext.name}`, () => {
  it("creates an anonymous user context", () => {
    const context = createUserContext(null, mockLocale);
    expect(context).toEqual({
      kind: "user",
      anonymous: true,
      locale: mockLocale,
    });
  });

  it("creates an identified user context", () => {
    const mockUser: UserRead = {
      userId: uuidV4(),
      name: "John Doe",
      email: "john.doe@airbyte.io",
      defaultWorkspaceId: "123",
      metadata: {},
    };
    const context = createUserContext(mockUser, mockLocale);
    expect(context).toEqual({
      kind: "user",
      anonymous: false,
      key: mockUser.userId,
      email: mockUser.email,
      name: mockUser.name,
      locale: mockLocale,
    });
  });
});

describe(`${createLDContext.name}`, () => {
  it("creates a workspace context", () => {
    const mockWorkspaceId = uuidV4();
    const context = createLDContext("workspace", mockWorkspaceId);
    expect(context).toEqual({
      kind: "workspace",
      key: mockWorkspaceId,
    });
  });
});

describe(`${createMultiContext.name}`, () => {
  it("creates a workspace context", () => {
    const mockWorkspaceId = uuidV4();
    const userContext = createUserContext(null, mockLocale);
    const workspaceContext = createLDContext("workspace", mockWorkspaceId);
    const multiContext = createMultiContext(userContext, workspaceContext);

    expect(multiContext).toEqual({
      kind: "multi",
      user: userContext,
      workspace: workspaceContext,
    });
  });
});

describe(`${getSingleContextsFromMulti.name}`, () => {
  it("gets single contexts from a multi context", () => {
    const mockWorkspaceId = uuidV4();
    const userContext = createUserContext(null, mockLocale);
    const workspaceContext = createLDContext("workspace", mockWorkspaceId);
    const multiContext = createMultiContext(userContext, workspaceContext);
    const singleContexts = getSingleContextsFromMulti(multiContext);
    expect(singleContexts).toEqual([userContext, workspaceContext]);
  });
});
