import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import "@testing-library/jest-dom";
import { ConnectorForm } from "../src/views/Connector/ConnectorForm/ConnectorForm";

// Mocks de dependencias
jest.mock("../src/hooks/services/FormChangeTracker", () => ({
  useFormChangeTrackerService: () => ({ clearFormChange: jest.fn() }),
  useUniqueFormId: () => "mock-form-id",
}));

jest.mock("../src/core/services/features", () => ({
  useFeature: () => false,
  FeatureItem: { ConnectorResourceAllocation: "mock-feature" },
}));

jest.mock("../src/core/services/embedded", () => ({
  useIsAirbyteEmbeddedContext: () => false,
}));

jest.mock("../src/views/Connector/ConnectorForm/useBuildForm", () => ({
  useBuildForm: () => ({
    formFields: [{ path: "name", label: "Name", type: "string" }],
    initialValues: { name: "" },
    validationSchema: { cast: (v: any) => v },
    groups: [],
  }),
}));

jest.mock("../src/core/utils/form", () => ({
  removeEmptyProperties: (v: any) => v,
}));

jest.mock("components/forms", () => ({
  Form: ({ children, onSubmit }: any) => (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        onSubmit({ name: "Test" });
      }}
    >
      {children}
      <button type="submit">Submit</button>
    </form>
  ),
}));

jest.mock("../src/views/Connector/ConnectorForm/FormRoot", () => ({
  FormRoot: () => <div data-testid="form-root">FormRoot rendered</div>,
}));

jest.mock("../src/views/Connector/ConnectorForm/connectorFormContext", () => ({
  ConnectorFormContextProvider: ({ children }: any) => <>{children}</>,
}));


describe("ConnectorForm", () => {
  const onSubmitMock = jest.fn().mockResolvedValue(undefined);

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renderizado correcto de form root", () => {
    render(
      <ConnectorForm
        formType="source"
        selectedConnectorDefinition={{ name: "Mock Connector" } as any}
        onSubmit={onSubmitMock}
        canEdit={true}
      />
    );

    expect(screen.getByTestId("form-root")).toBeInTheDocument();
  });

  it("llama onSubmit cuando el form es subido", () => {
    render(
      <ConnectorForm
        formType="source"
        selectedConnectorDefinition={{ name: "Mock Connector" } as any}
        onSubmit={onSubmitMock}
        canEdit={true}
      />
    );

    fireEvent.click(screen.getByText("Submit"));

    expect(onSubmitMock).toHaveBeenCalledWith({ name: "Test" });
  });
});
