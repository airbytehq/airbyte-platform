import { act, cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import isEqual from "lodash/isEqual";
import { IntlProvider } from "react-intl";

import { mockSource } from "test-utils";
import {
  mockDestinationDefinition,
  mockDestinationDefinitionSpecification,
} from "test-utils/mock-data/mockDestination";
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
let mockOnCheckSection: ((values: typeof formValues) => Promise<unknown>) | undefined;

jest.mock("area/connector/components/ConnectorForm", () => ({
  ConnectorForm: ({
    onSubmit,
    onCheckSection,
    formHeader,
    renderFooter,
  }: {
    onSubmit: (values: typeof formValues) => Promise<void>;
    onCheckSection: (values: typeof formValues) => Promise<unknown>;
    formHeader: React.ReactNode;
    renderFooter: (props: {
      dirty: boolean;
      isSubmitting: boolean;
      isValid: boolean;
      resetConnectorForm: jest.Mock;
      getValues: () => typeof formValues;
    }) => React.ReactNode;
  }) => {
    mockOnCheckSection = onCheckSection;
    return (
      <>
        {formHeader}
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
    );
  },
}));

jest.mock("./components/Controls", () => ({
  Controls: ({ onSaveDraft }: { onSaveDraft?: () => Promise<void> }) =>
    onSaveDraft ? (
      <button type="button" onClick={() => onSaveDraft()}>
        Save draft
      </button>
    ) : null,
}));

jest.mock("area/connector/components/ConnectorIcon", () => ({ ConnectorIcon: () => null }));

jest.mock("pages/connectorBuilder/components/ForkConnectorButton", () => ({
  ForkConnectorButton: ({ iconOnly, disabled }: { iconOnly?: boolean; disabled?: boolean }) => (
    <button type="button" disabled={disabled}>
      {iconOnly ? "Fork in Builder icon" : "Fork in Builder"}
    </button>
  ),
}));

jest.mock("area/organization/components/CloudInviteUsersButton", () => ({
  CloudInviteUsersButton: ({ disabled }: { disabled?: boolean }) => (
    <button type="button" disabled={disabled}>
      Invite users
    </button>
  ),
}));

jest.mock("core/api", () => ({
  CommandErrorWithJobInfo: { getJobInfo: jest.fn() },
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

const newSourceCardProps = {
  formType: "source" as const,
  availableConnectorDefinitions: [mockSourceDefinition],
  selectedConnectorDefinitionId: mockSourceDefinition.sourceDefinitionId,
  selectedConnectorDefinitionSpecification: mockSourceDefinitionSpecification,
  onSubmit: jest.fn(),
};

const renderCard = (props: React.ComponentProps<typeof ConnectorCard>, testConnector = jest.fn()) => {
  mockUseTestConnectorCommand.mockReturnValue({
    testConnector,
    isTestConnectionInProgress: false,
    onStopTesting: jest.fn(),
    error: null,
    reset: jest.fn(),
    isSuccess: false,
  });

  return render(<ConnectorCard {...props} />, {
    wrapper: ({ children }) => (
      <IntlProvider locale="en" messages={messages}>
        {children}
      </IntlProvider>
    ),
  });
};

describe("ConnectorCard header", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("shows the connector name with Fork in Builder and Invite users for a new source", () => {
    renderCard(newSourceCardProps);

    expect(screen.getByRole("heading", { name: mockSourceDefinition.name })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Fork in Builder icon" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Invite users" })).toBeInTheDocument();
  });

  it("shows the connector name with Invite users and without Fork in Builder for a new destination", () => {
    renderCard({
      ...newSourceCardProps,
      formType: "destination",
      availableConnectorDefinitions: [mockDestinationDefinition],
      selectedConnectorDefinitionId: mockDestinationDefinition.destinationDefinitionId,
      selectedConnectorDefinitionSpecification: mockDestinationDefinitionSpecification,
    });

    expect(screen.getByRole("heading", { name: mockDestinationDefinition.name })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Fork in Builder/ })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Invite users" })).toBeInTheDocument();
  });

  it("shows no header in edit mode", () => {
    renderCard({
      ...newSourceCardProps,
      isEditMode: true,
      connector: { ...mockSource, sourceDefinitionId: mockSourceDefinition.sourceDefinitionId },
    });

    expect(screen.queryByRole("heading", { name: mockSourceDefinition.name })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Fork in Builder/ })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Invite users" })).not.toBeInTheDocument();
  });
});

describe("ConnectorCard while the spec loads", () => {
  const loadingSourceCardProps = {
    ...newSourceCardProps,
    selectedConnectorDefinitionSpecification: undefined,
    isLoading: true,
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
    jest.useRealTimers();
  });

  it("shows the logo animation instead of the form", () => {
    renderCard(loadingSourceCardProps);

    expect(within(screen.getByRole("status")).getByTitle("Loading …")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Submit form" })).not.toBeInTheDocument();
  });

  it("shows the form once the spec loads", () => {
    const { rerender } = renderCard(loadingSourceCardProps);

    rerender(<ConnectorCard {...newSourceCardProps} />);

    expect(screen.queryByRole("status")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Submit form" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Fork in Builder icon" })).toBeEnabled();
  });

  it("shows the header with its actions disabled", () => {
    renderCard(loadingSourceCardProps);

    expect(screen.getByRole("heading", { name: mockSourceDefinition.name })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Fork in Builder icon" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Invite users" })).toBeDisabled();
  });

  it("shows the slow-load support link only after the timeout", () => {
    jest.useFakeTimers();
    renderCard(loadingSourceCardProps);

    expect(screen.queryByText(/Taking too long\?/)).not.toBeInTheDocument();

    act(() => {
      jest.advanceTimersByTime(10000);
    });

    expect(screen.getByText(/Taking too long\?/)).toHaveTextContent(/^Taking too long\? See here for support$/);
    expect(screen.getByRole("link", { name: "here" })).toBeInTheDocument();
  });
});

describe("ConnectorCard section check", () => {
  const sectionValues = {
    ...formValues,
    connectionConfiguration: { host: "db.example.com", "check-section": "connection" },
  };

  // ConnectorCard calls useTestConnectorCommand twice: with all its props for Save & Test, and with only the form type
  // for the section check
  const renderCardWithSectionCheck = (sectionTestConnector: jest.Mock) => {
    const saveAndTestConnector = jest.fn();
    mockUseTestConnectorCommand.mockImplementation((commandProps) => ({
      testConnector: isEqual(commandProps, { formType: commandProps.formType })
        ? sectionTestConnector
        : saveAndTestConnector,
      isTestConnectionInProgress: false,
      onStopTesting: jest.fn(),
      error: null,
      reset: jest.fn(),
      isSuccess: false,
    }));

    render(<ConnectorCard {...newSourceCardProps} />, {
      wrapper: ({ children }) => (
        <IntlProvider locale="en" messages={messages}>
          {children}
        </IntlProvider>
      ),
    });
    return { saveAndTestConnector };
  };

  beforeEach(() => {
    jest.clearAllMocks();
    mockOnCheckSection = undefined;
  });

  it("checks the section values with the connector's service type in its own check, not in the Save & Test one", async () => {
    const sectionTestConnector = jest.fn().mockResolvedValue({ id: "command-id", status: "succeeded" });
    const { saveAndTestConnector } = renderCardWithSectionCheck(sectionTestConnector);

    await mockOnCheckSection?.(sectionValues);
    expect(sectionTestConnector).toHaveBeenCalledWith({
      ...sectionValues,
      serviceType: mockSourceDefinition.sourceDefinitionId,
    });
    expect(saveAndTestConnector).not.toHaveBeenCalled();
  });

  it("rejects with the error of a failed check", async () => {
    const checkError = new Error("The host db.example.com is not reachable.");
    renderCardWithSectionCheck(jest.fn().mockRejectedValue(checkError));

    await expect(mockOnCheckSection?.(sectionValues)).rejects.toBe(checkError);
  });

  it("rejects with a generic message when the check was aborted", async () => {
    renderCardWithSectionCheck(jest.fn().mockResolvedValue({ id: "command-id", status: "failed" }));

    await expect(mockOnCheckSection?.(sectionValues)).rejects.toThrow(messages["connector.check.failed"]);
  });
});
