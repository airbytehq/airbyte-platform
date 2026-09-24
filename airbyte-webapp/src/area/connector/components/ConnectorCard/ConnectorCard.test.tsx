import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { IntlProvider } from "react-intl";

import { mockSource } from "test-utils";
import { mockSourceDefinition, mockSourceDefinitionSpecification } from "test-utils/mock-data/mockSource";

import { useTestConnectorCommand } from "core/api";
import messages from "locales/en.json";

import { ConnectorCard } from "./ConnectorCard";

const formValues = {
  name: "Draft source",
  connectionConfiguration: {},
  resourceAllocation: {},
};

const mockTrackTestConnectorFailure = jest.fn();
const mockTrackTestConnectorStarted = jest.fn();
const mockTrackTestConnectorSuccess = jest.fn();

jest.mock("area/connector/components/ConnectorForm", () => ({
  ConnectorForm: ({
    onSubmit,
    renderFooter,
  }: {
    onSubmit: (values: typeof formValues) => Promise<void>;
    renderFooter: (props: {
      dirty: boolean;
      isSubmitting: boolean;
      isValid: boolean;
      resetConnectorForm: jest.Mock;
      getValues: () => typeof formValues;
    }) => React.ReactNode;
  }) => (
    <>
      <button type="button" onClick={() => onSubmit(formValues).catch(() => undefined)}>
        Submit form
      </button>
      {renderFooter({
        dirty: true,
        isSubmitting: false,
        isValid: false,
        resetConnectorForm: jest.fn(),
        getValues: () => formValues,
      })}
    </>
  ),
}));

jest.mock("./components/Controls", () => ({
  Controls: ({ onSaveDraft }: { onSaveDraft?: () => Promise<void> }) =>
    onSaveDraft ? (
      <button type="button" onClick={() => onSaveDraft()}>
        Save draft
      </button>
    ) : null,
}));

jest.mock("core/api", () => ({
  CommandErrorWithJobInfo: { getJobInfo: jest.fn() },
  useCurrentWorkspace: () => ({ workspaceId: "workspace-id" }),
  useTestConnectorCommand: jest.fn(),
}));

jest.mock("core/services/Notification", () => ({
  useNotificationService: () => ({ registerNotification: jest.fn() }),
}));
jest.mock("core/services/Experiment", () => ({ useExperiment: () => false }));
jest.mock("core/utils/app", () => ({ useIsCloudApp: () => false }));
jest.mock("core/utils/rbac", () => ({ Intent: {}, useGeneratedIntent: () => true }));
jest.mock("./useAnalyticsTrackFunctions", () => ({
  useAnalyticsTrackFunctions: () => ({
    trackTestConnectorFailure: mockTrackTestConnectorFailure,
    trackTestConnectorStarted: mockTrackTestConnectorStarted,
    trackTestConnectorSuccess: mockTrackTestConnectorSuccess,
  }),
}));
jest.mock("../ConnectorDocumentationLayout/DocumentationPanelContext", () => ({
  useDocumentationPanelContext: () => ({
    setDocumentationPanelOpen: jest.fn(),
    setSelectedConnectorDefinition: jest.fn(),
  }),
}));

const mockUseTestConnectorCommand = useTestConnectorCommand as jest.MockedFunction<typeof useTestConnectorCommand>;

const renderDraftCard = async (testConnector: jest.Mock) => {
  const saveDraft = jest.fn().mockResolvedValue({ ...mockSource, isDraft: true });
  const onDraftPromoted = jest.fn().mockResolvedValue(undefined);
  mockUseTestConnectorCommand.mockReturnValue({
    testConnector,
    isTestConnectionInProgress: false,
    onStopTesting: jest.fn(),
    error: null,
    reset: jest.fn(),
    isSuccess: false,
  });

  render(
    <IntlProvider locale="en" messages={messages}>
      <ConnectorCard
        formType="source"
        availableConnectorDefinitions={[
          { ...mockSourceDefinition, sourceDefinitionId: mockSource.sourceDefinitionId, name: mockSource.sourceName },
        ]}
        selectedConnectorDefinitionId={mockSource.sourceDefinitionId}
        selectedConnectorDefinitionSpecification={{
          ...mockSourceDefinitionSpecification,
          sourceDefinitionId: mockSource.sourceDefinitionId,
          connectionSpecification: { type: "object", required: ["token"], properties: { token: { type: "string" } } },
        }}
        onSubmit={jest.fn()}
        onSaveDraft={saveDraft}
        onDraftPromoted={onDraftPromoted}
      />
    </IntlProvider>
  );

  return { saveDraft, onDraftPromoted };
};

describe("ConnectorCard draft setup", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("persists a draft before checking by actor ID and retains it after a failed check", async () => {
    const testConnector = jest.fn().mockRejectedValue(new Error("check failed"));
    const { saveDraft, onDraftPromoted } = await renderDraftCard(testConnector);

    await userEvent.click(screen.getByRole("button", { name: "Submit form" }));

    expect(saveDraft).toHaveBeenCalledWith(expect.objectContaining(formValues), undefined);
    expect(testConnector).toHaveBeenCalledWith(undefined, expect.objectContaining({ sourceId: mockSource.sourceId }));
    expect(saveDraft.mock.invocationCallOrder[0]).toBeLessThan(testConnector.mock.invocationCallOrder[0]);
    expect(onDraftPromoted).not.toHaveBeenCalled();

    await userEvent.click(screen.getByRole("button", { name: "Save draft" }));
    expect(saveDraft).toHaveBeenLastCalledWith(
      expect.objectContaining(formValues),
      expect.objectContaining({ sourceId: mockSource.sourceId })
    );
  });

  it("continues only after an actor-ID check succeeds", async () => {
    const testConnector = jest.fn().mockResolvedValue({ id: "command-id", status: "succeeded" });
    const { onDraftPromoted } = await renderDraftCard(testConnector);

    await userEvent.click(screen.getByRole("button", { name: "Submit form" }));

    expect(onDraftPromoted).toHaveBeenCalledWith(
      expect.objectContaining({ sourceId: mockSource.sourceId }),
      expect.objectContaining(formValues)
    );
  });

  it("keeps the actor as a draft when an actor-ID check is cancelled", async () => {
    const testConnector = jest.fn().mockResolvedValue({ id: "command-id", status: "failed" });
    const { saveDraft, onDraftPromoted } = await renderDraftCard(testConnector);

    await userEvent.click(screen.getByRole("button", { name: "Submit form" }));

    expect(saveDraft).toHaveBeenCalledTimes(1);
    expect(testConnector).toHaveBeenCalledWith(undefined, expect.objectContaining({ sourceId: mockSource.sourceId }));
    expect(onDraftPromoted).not.toHaveBeenCalled();
    expect(mockTrackTestConnectorSuccess).not.toHaveBeenCalled();
  });
});
