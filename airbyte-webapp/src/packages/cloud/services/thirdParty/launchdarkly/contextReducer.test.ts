import { LDMultiKindContext } from "launchdarkly-js-client-sdk";

import {
  mockSourceDefinitionContext,
  mockUserContext,
  mockWorkspaceContext,
} from "test-utils/mock-data/mockLaunchDarklyContext";

import { LDContextReducerAction, contextReducer } from "./contextReducer";
import { createMultiContext } from "./contexts";

describe(`${contextReducer.name}`, () => {
  it("should add a context", () => {
    const state = {
      context: createMultiContext(mockUserContext),
    };

    const action: LDContextReducerAction = {
      type: "add",
      context: mockWorkspaceContext,
    };

    const result = contextReducer(state, action);

    const expectedMultiContext: LDMultiKindContext = {
      kind: "multi",
      user: mockUserContext,
      workspace: mockWorkspaceContext,
    };

    expect(result).toEqual({
      context: expectedMultiContext,
    });
  });

  it("should add multiple contexts", () => {
    const state = {
      context: createMultiContext(mockUserContext),
    };

    const actionAddWorkspace: LDContextReducerAction = {
      type: "add",
      context: mockWorkspaceContext,
    };

    const actionAddSourceDefinition: LDContextReducerAction = {
      type: "add",
      context: mockSourceDefinitionContext,
    };

    const intermediateState = contextReducer(state, actionAddWorkspace);
    const result = contextReducer(intermediateState, actionAddSourceDefinition);

    const expectedMultiContext: LDMultiKindContext = {
      kind: "multi",
      user: mockUserContext,
      workspace: mockWorkspaceContext,
      sourceDefinition: mockSourceDefinitionContext,
    };

    expect(result).toEqual({
      context: expectedMultiContext,
    });
  });

  it("should remove a context", () => {
    const state = {
      context: createMultiContext(mockUserContext, mockWorkspaceContext),
    };

    const action: LDContextReducerAction = {
      type: "remove",
      kind: "workspace",
    };

    const result = contextReducer(state, action);

    const expectedMultiContext: LDMultiKindContext = {
      kind: "multi",
      user: mockUserContext,
    };

    expect(result).toEqual({
      context: expectedMultiContext,
    });
  });

  it("state object has a stable reference if a duplicate context is added", () => {
    const state = {
      context: createMultiContext(mockUserContext),
    };
    const action: LDContextReducerAction = {
      type: "add",
      context: mockUserContext,
    };
    const result = contextReducer(state, action);
    expect(result).toBe(state);
  });
});
