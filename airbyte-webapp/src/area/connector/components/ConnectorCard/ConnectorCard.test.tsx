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

import { CheckSection } from "area/connector/components/ConnectorForm";
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
let mockOnCheckSection: CheckSection | undefined;
const mockTestConnectTarget = jest.fn();
const mockMarkFormAsSaved = jest.fn();

jest.mock("area/connector/components/ConnectorForm", () => ({
  ConnectorForm: ({
    onSubmit,
    onCheckSection,
    formHeader,
    isSavingDraft,
    renderFooter,
  }: {
    onSubmit: (values: typeof formValues) => Promise<void>;
    onCheckSection: CheckSection;
    formHeader: React.ReactNode;
    isSavingDraft?: boolean;
    renderFooter: (props: {
      dirty: boolean;
      isSubmitting: boolean;
      isValid: boolean;
      resetConnectorForm: jest.Mock;
      getValues: () => typeof formValues;
      markFormAsSaved: () => void;
    }) => React.ReactNode;
  }) => {
    mockOnCheckSection = onCheckSection;
    return (
      <>
        {formHeader}
        <span data-testid="form-saving-draft">{String(Boolean(isSavingDraft))}</span>
        <button type="button" onClick={() => onSubmit(formValues).catch(() => undefined)}>
          Submit form
        </button>
        {renderFooter({
          dirty: true,
          isSubmitting: false,
          isValid: false,
          resetConnectorForm: jest.fn(),
          getValues: () => formValues,
          markFormAsSaved: mockMarkFormAsSaved,
        })}
      </>
    );
  },
}));

jest.mock("./components/Controls", () => ({
  Controls: ({ onSaveDraft }: { onSaveDraft?: () => Promise<void> }) =>
    onSaveDraft ? (
      <button type="button" onClick={() => onSaveDraft().catch(() => undefined)}>
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
  useConnectCommand: () => mockTestConnectTarget,
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

const renderDraftCard = async (
  testConnector: jest.Mock,
  saveDraft: jest.Mock = jest.fn().mockResolvedValue({ ...mockSource, isDraft: true })
) => {
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

describe("ConnectorCard save as a draft", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("while the draft is saving", () => {
    const renderPendingSave = async () => {
      let settleSave!: { resolve: (draft: unknown) => void; reject: (error: Error) => void };
      const saveDraft = jest.fn(
        () =>
          new Promise((resolve, reject) => {
            settleSave = { resolve, reject };
          })
      );
      await renderDraftCard(jest.fn(), saveDraft);
      await userEvent.click(screen.getByRole("button", { name: "Save draft" }));

      return settleSave;
    };
    const formSavingDraft = () => screen.getByTestId("form-saving-draft");

    it("disables the form", async () => {
      const settleSave = await renderPendingSave();

      expect(formSavingDraft()).toHaveTextContent("true");

      await act(async () => settleSave.resolve({ ...mockSource, isDraft: true }));
    });

    it("enables the form again once the draft saved", async () => {
      const settleSave = await renderPendingSave();

      await act(async () => settleSave.resolve({ ...mockSource, isDraft: true }));

      expect(formSavingDraft()).toHaveTextContent("false");
    });

    it("enables the form again when the draft fails to save", async () => {
      const settleSave = await renderPendingSave();

      await act(async () => settleSave.reject(new Error("save failed")));

      expect(formSavingDraft()).toHaveTextContent("false");
    });
  });

  it("marks the form as saved once the draft saves", async () => {
    await renderDraftCard(jest.fn());

    await userEvent.click(screen.getByRole("button", { name: "Save draft" }));

    expect(mockMarkFormAsSaved).toHaveBeenCalledTimes(1);
  });

  it("does not mark the form as saved when the draft fails to save", async () => {
    await renderDraftCard(jest.fn(), jest.fn().mockRejectedValue(new Error("save failed")));

    await userEvent.click(screen.getByRole("button", { name: "Save draft" }));

    expect(mockMarkFormAsSaved).not.toHaveBeenCalled();
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

  describe("with a Connect target", () => {
    const connectTarget = { host: "db.example.com", port: 5432 };

    it("passes the check with the Connect command and runs no connector check", async () => {
      const sectionTestConnector = jest.fn();
      mockTestConnectTarget.mockResolvedValue({ id: "command-id", reachable: true, error_code: "none" });
      renderCardWithSectionCheck(sectionTestConnector);

      await mockOnCheckSection?.(sectionValues, connectTarget);

      expect(mockTestConnectTarget).toHaveBeenCalledWith(connectTarget);
      expect(sectionTestConnector).not.toHaveBeenCalled();
    });

    it("fails the check with the message for its error code", async () => {
      const sectionTestConnector = jest.fn();
      mockTestConnectTarget.mockResolvedValue({ id: "command-id", reachable: false, error_code: "dns_failed" });
      renderCardWithSectionCheck(sectionTestConnector);

      await expect(mockOnCheckSection?.(sectionValues, connectTarget)).rejects.toThrow(
        new Error("Airbyte could not find the host db.example.com. Check the host name.")
      );
      expect(sectionTestConnector).not.toHaveBeenCalled();
    });

    it("runs the section check when the server refuses the Connect command", async () => {
      const sectionTestConnector = jest.fn().mockResolvedValue({ id: "command-id", status: "succeeded" });
      mockTestConnectTarget.mockResolvedValue(undefined);
      renderCardWithSectionCheck(sectionTestConnector);

      await mockOnCheckSection?.(sectionValues, connectTarget);

      expect(sectionTestConnector).toHaveBeenCalledWith({
        ...sectionValues,
        serviceType: mockSourceDefinition.sourceDefinitionId,
      });
    });

    it("runs the section check when the Connect command fails on the platform side", async () => {
      const sectionTestConnector = jest.fn().mockResolvedValue({ id: "command-id", status: "succeeded" });
      mockTestConnectTarget.mockResolvedValue({
        id: "command-id",
        reachable: false,
        error_code: "unknown",
        failure_reason: { externalMessage: "The connect test failed" },
      });
      renderCardWithSectionCheck(sectionTestConnector);

      await mockOnCheckSection?.(sectionValues, connectTarget);

      expect(sectionTestConnector).toHaveBeenCalledWith({
        ...sectionValues,
        serviceType: mockSourceDefinition.sourceDefinitionId,
      });
    });

    it("fails the check with any other error from the Connect command, and runs no section check", async () => {
      const blockedAddressError = new Error("Destination is not allowed");
      const sectionTestConnector = jest.fn();
      mockTestConnectTarget.mockRejectedValue(blockedAddressError);
      renderCardWithSectionCheck(sectionTestConnector);

      await expect(mockOnCheckSection?.(sectionValues, connectTarget)).rejects.toBe(blockedAddressError);
      expect(sectionTestConnector).not.toHaveBeenCalled();
    });
  });
});
