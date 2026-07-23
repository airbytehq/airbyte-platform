import { renderHook } from "@testing-library/react";

import { type ConnectorFormValues } from "area/connector/components/ConnectorForm/types";

import { useSubmitConfigurationTool } from "./useSubmitConfigurationTool";

describe("useSubmitConfigurationTool", () => {
  const actorDefinitionId = "actor-def-id";

  const buildFormValues = (overrides: Partial<ConnectorFormValues> = {}): ConnectorFormValues =>
    ({
      name: "Typeform",
      connectionConfiguration: { token: "abc" },
      ...overrides,
    }) as ConnectorFormValues;

  it("uses the agent-supplied name when provided", () => {
    const onSubmitSourceStep = jest.fn();
    const getFormValues = jest.fn(() => buildFormValues());

    const { result } = renderHook(() =>
      useSubmitConfigurationTool({ actorDefinitionId, onSubmitSourceStep, getFormValues })
    );
    result.current.execute({ name: "MyCustomName", configuration: { token: "abc" } }, jest.fn());

    expect(onSubmitSourceStep).toHaveBeenCalledTimes(1);
    expect(onSubmitSourceStep).toHaveBeenCalledWith({
      name: "MyCustomName",
      serviceType: actorDefinitionId,
      connectionConfiguration: { token: "abc" },
    });
  });

  it("trims whitespace from the agent-supplied name", () => {
    const onSubmitSourceStep = jest.fn();
    const getFormValues = jest.fn(() => buildFormValues());

    const { result } = renderHook(() =>
      useSubmitConfigurationTool({ actorDefinitionId, onSubmitSourceStep, getFormValues })
    );
    result.current.execute({ name: "  Padded Name  " }, jest.fn());

    expect(onSubmitSourceStep).toHaveBeenCalledWith(expect.objectContaining({ name: "Padded Name" }));
  });

  it("falls back to the form name when the agent does not supply a name", () => {
    const onSubmitSourceStep = jest.fn();
    const getFormValues = jest.fn(() => buildFormValues({ name: "FormName" }));

    const { result } = renderHook(() =>
      useSubmitConfigurationTool({ actorDefinitionId, onSubmitSourceStep, getFormValues })
    );
    result.current.execute({ configuration: { token: "abc" } }, jest.fn());

    expect(onSubmitSourceStep).toHaveBeenCalledWith(expect.objectContaining({ name: "FormName" }));
  });

  it("falls back to the form name when the agent supplies an empty/whitespace name", () => {
    const onSubmitSourceStep = jest.fn();
    const getFormValues = jest.fn(() => buildFormValues({ name: "FormName" }));

    const { result } = renderHook(() =>
      useSubmitConfigurationTool({ actorDefinitionId, onSubmitSourceStep, getFormValues })
    );
    result.current.execute({ name: "   " }, jest.fn());

    expect(onSubmitSourceStep).toHaveBeenCalledWith(expect.objectContaining({ name: "FormName" }));
  });

  it("falls back to the form name when args is null/undefined", () => {
    const onSubmitSourceStep = jest.fn();
    const getFormValues = jest.fn(() => buildFormValues({ name: "FormName" }));

    const { result } = renderHook(() =>
      useSubmitConfigurationTool({ actorDefinitionId, onSubmitSourceStep, getFormValues })
    );
    result.current.execute(null, jest.fn());

    expect(onSubmitSourceStep).toHaveBeenCalledWith(expect.objectContaining({ name: "FormName" }));
  });

  it("does not call onSubmitSourceStep when the form has no configuration", () => {
    const onSubmitSourceStep = jest.fn();
    const getFormValues = jest.fn(
      () => ({ name: "Typeform", connectionConfiguration: undefined }) as unknown as ConnectorFormValues
    );

    const { result } = renderHook(() =>
      useSubmitConfigurationTool({ actorDefinitionId, onSubmitSourceStep, getFormValues })
    );
    const consoleErrorSpy = jest.spyOn(console, "error").mockImplementation(() => undefined);
    result.current.execute({ name: "MyCustomName" }, jest.fn());

    expect(onSubmitSourceStep).not.toHaveBeenCalled();
    consoleErrorSpy.mockRestore();
  });
});
