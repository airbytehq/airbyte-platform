import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { FormProvider, useForm, useFormState, useWatch } from "react-hook-form";

import { TestWrapper } from "test-utils";

import { FormConnectionFormValues } from "area/connection/components/ConnectionForm/formConfig";
import { useConnectionFormService } from "area/connection/utils/ConnectionForm/ConnectionFormService";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { ConnectionSemanticSearchAccess } from "cloud/components/AgentsOptIn";
import { useConnectionSemanticSearchAccess } from "cloud/components/AgentsOptIn/useConnectionSemanticSearchAccess";
import { useConnectionSemanticSearchEnablement, useUpdateConnectionSemanticSearchEnablement } from "core/api";
import { useIntent } from "core/utils/rbac";

import { SemanticSearchFormField } from "./SemanticSearchFormField";

jest.mock("area/connection/utils/ConnectionForm/ConnectionFormService", () => ({
  useConnectionFormService: jest.fn(),
}));
jest.mock("area/workspace/utils", () => ({ useCurrentWorkspaceId: jest.fn() }));
jest.mock("cloud/components/AgentsOptIn/useConnectionSemanticSearchAccess", () => ({
  useConnectionSemanticSearchAccess: jest.fn(),
}));
jest.mock("core/api", () => ({
  useConnectionSemanticSearchEnablement: jest.fn(),
  useUpdateConnectionSemanticSearchEnablement: jest.fn(),
}));
jest.mock("core/utils/rbac", () => ({
  ...jest.requireActual("core/utils/rbac"),
  useIntent: jest.fn(),
}));

const mockUseConnectionFormService = useConnectionFormService as jest.MockedFunction<typeof useConnectionFormService>;
const mockUseCurrentWorkspaceId = useCurrentWorkspaceId as jest.MockedFunction<typeof useCurrentWorkspaceId>;
const mockUseAccess = useConnectionSemanticSearchAccess as jest.MockedFunction<
  typeof useConnectionSemanticSearchAccess
>;
const mockUseEnablement = useConnectionSemanticSearchEnablement as jest.MockedFunction<
  typeof useConnectionSemanticSearchEnablement
>;
const mockUseUpdateEnablement = useUpdateConnectionSemanticSearchEnablement as jest.MockedFunction<
  typeof useUpdateConnectionSemanticSearchEnablement
>;
const mockUseIntent = useIntent as jest.MockedFunction<typeof useIntent>;

const SWITCH_NAME = "GitHub → Snowflake semantic search";

const mockSavedValue = (enabled: boolean) =>
  mockUseEnablement.mockReturnValue({
    data: { connectionId: "connection-1", enable_indexing: enabled },
    isLoading: false,
    isError: false,
  } as never);

const mockUpdate = (overrides: Record<string, unknown> = {}) => {
  const update = { mutateAsync: jest.fn().mockResolvedValue(undefined), isLoading: false, ...overrides };
  mockUseUpdateEnablement.mockReturnValue(update as never);
  return update;
};

// The intent and the access hook agree in the app: "noPermission" means the intent is false.
const mockAccess = (access: ConnectionSemanticSearchAccess) => {
  mockUseAccess.mockReturnValue(access);
  mockUseIntent.mockReturnValue(access !== "noPermission");
};

const renderField = (props: { isCreating?: boolean; disabled?: boolean } = {}) =>
  render(
    <TestWrapper>
      <SemanticSearchFormField
        isCreating={props.isCreating ?? false}
        sourceDefinitionId="source-definition"
        destinationDefinitionId="destination-definition"
        disabled={props.disabled}
      />
    </TestWrapper>
  );

// Shows the form state that the create flow relies on: the field value and whether the form is dirty.
const FormStateProbe: React.FC = () => {
  const value = useWatch<FormConnectionFormValues, "semanticSearch">({ name: "semanticSearch" });
  const { isDirty } = useFormState<FormConnectionFormValues>();

  return (
    <>
      <span data-testid="semantic-search-value">{String(value)}</span>
      <span data-testid="form-dirty">{String(isDirty)}</span>
    </>
  );
};

// The create step renders the field inside the connection form. The initial values have no semantic search value.
const CreateStepForm: React.FC<{ showField?: boolean }> = ({ showField = true }) => {
  const methods = useForm<FormConnectionFormValues>({
    defaultValues: { name: "GitHub → Snowflake" } as FormConnectionFormValues,
  });

  return (
    <FormProvider {...methods}>
      {/* The probe comes first, so it subscribes before the field sets its default, like the mounted form does. */}
      <FormStateProbe />
      {showField && (
        <SemanticSearchFormField
          isCreating
          sourceDefinitionId="source-definition"
          destinationDefinitionId="destination-definition"
        />
      )}
    </FormProvider>
  );
};

const renderCreateStep = () => {
  const result = render(
    <TestWrapper>
      <CreateStepForm />
    </TestWrapper>
  );
  const rerender = (showField = true) =>
    result.rerender(
      <TestWrapper>
        <CreateStepForm showField={showField} />
      </TestWrapper>
    );
  return { rerender };
};

const expectFormState = async ({ value, isDirty }: { value: string; isDirty: boolean }) => {
  await waitFor(() => expect(screen.getByTestId("semantic-search-value")).toHaveTextContent(value));
  expect(screen.getByTestId("form-dirty")).toHaveTextContent(String(isDirty));
};

describe("SemanticSearchFormField", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseConnectionFormService.mockReturnValue({
      connection: { connectionId: "connection-1", name: "GitHub → Snowflake" },
    } as never);
    mockUseCurrentWorkspaceId.mockReturnValue("workspace-1");
    mockAccess("editable");
    mockSavedValue(true);
    mockUpdate();
  });

  it("renders nothing when the access is hidden", () => {
    mockAccess("hidden");

    renderField();

    expect(mockUseAccess).toHaveBeenCalledWith({
      sourceDefinitionId: "source-definition",
      destinationDefinitionId: "destination-definition",
    });
    expect(screen.queryByText("Semantic search")).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
    expect(mockUseEnablement).not.toHaveBeenCalled();
  });

  it("shows the label and the description", () => {
    renderField();

    expect(screen.getByText("Semantic search")).toBeVisible();
    expect(
      screen.getByText(
        "Agents can use the Airbyte MCP to search and reason about your data with greater accuracy and efficiency."
      )
    ).toBeVisible();
  });

  it("locks the switch off and says an organization admin can turn it on when the organization setting is off", () => {
    mockAccess("orgOff");

    renderField();

    const control = screen.getByRole("checkbox");
    expect(control).toBeDisabled();
    expect(control).not.toBeChecked();
    expect(mockUseEnablement).not.toHaveBeenCalled();
    fireEvent.mouseOver(control);
    expect(
      screen.getByText(
        "Semantic search is off for this organization. An organization admin can turn it on in Context layer settings."
      )
    ).toBeVisible();
    expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });

  it("shows the saved value in a disabled switch with a tooltip when the viewer cannot edit", () => {
    mockAccess("noPermission");

    renderField();

    const control = screen.getByRole("checkbox", { name: SWITCH_NAME });
    expect(control).toBeDisabled();
    expect(control).toBeChecked();
    expect(mockUseIntent).toHaveBeenCalledWith("EditConnectionSemanticSearch", { workspaceId: "workspace-1" });
    fireEvent.mouseOver(control);
    expect(screen.getByText("You need edit permission for this workspace to change semantic search.")).toBeVisible();
  });

  it("disables the switch for a deprecated connection", () => {
    renderField({ disabled: true });

    expect(screen.getByRole("checkbox", { name: SWITCH_NAME })).toBeDisabled();
  });

  it("saves at once when the viewer turns semantic search on", async () => {
    mockSavedValue(false);
    const update = mockUpdate();

    renderField();
    const control = screen.getByRole("checkbox", { name: SWITCH_NAME });
    expect(control).toBeEnabled();
    fireEvent.click(control);

    await waitFor(() => expect(update.mutateAsync).toHaveBeenCalledWith({ enabled: true, expectedEnabled: false }));
    expect(screen.queryByText("Turn off semantic search")).not.toBeInTheDocument();
  });

  it("asks for confirmation before it turns semantic search off", async () => {
    const update = mockUpdate();

    renderField();
    fireEvent.click(screen.getByRole("checkbox", { name: SWITCH_NAME }));

    expect(await screen.findByText("Turn off semantic search")).toBeVisible();
    expect(
      screen.getByText(
        "Turning off semantic search also deletes your semantic index. If you enable this again, you will need to backfill your data again, which will incur additional compute costs on your destination."
      )
    ).toBeVisible();
    expect(update.mutateAsync).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByText("Turn off semantic search")).not.toBeInTheDocument());
    expect(update.mutateAsync).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("checkbox", { name: SWITCH_NAME }));
    fireEvent.click(await screen.findByRole("button", { name: "Delete semantic index" }));

    await waitFor(() => expect(update.mutateAsync).toHaveBeenCalledWith({ enabled: false, expectedEnabled: true }));
  });

  describe("on the create step", () => {
    it.each(["loading", "hidden"] as const)(
      "renders nothing and sets no value when the access is %s",
      async (access) => {
        mockAccess(access);

        renderCreateStep();

        expect(mockUseAccess).toHaveBeenCalledWith({
          sourceDefinitionId: "source-definition",
          destinationDefinitionId: "destination-definition",
        });
        expect(screen.queryByText("Semantic search")).not.toBeInTheDocument();
        expect(screen.queryByRole("checkbox")).not.toBeInTheDocument();
        await expectFormState({ value: "undefined", isDirty: false });
      }
    );

    it("turns semantic search on by default when the viewer can edit, and keeps the form clean", async () => {
      renderCreateStep();

      expect(screen.getByText("Semantic search")).toBeVisible();
      expect(
        screen.getByText(
          "Agents can use the Airbyte MCP to search and reason about your data with greater accuracy and efficiency."
        )
      ).toBeVisible();
      const control = screen.getByRole("checkbox");
      expect(control).toBeEnabled();
      expect(control).toBeChecked();
      await expectFormState({ value: "true", isDirty: false });
      // The create step reads no saved value and saves nothing on toggle.
      expect(mockUseEnablement).not.toHaveBeenCalled();
      expect(mockUseUpdateEnablement).not.toHaveBeenCalled();
    });

    it("sets the default once the access stops loading", async () => {
      mockAccess("loading");
      const { rerender } = renderCreateStep();
      await expectFormState({ value: "undefined", isDirty: false });

      mockAccess("editable");
      rerender();

      expect(screen.getByRole("checkbox")).toBeChecked();
      await expectFormState({ value: "true", isDirty: false });
    });

    it("keeps the value the user changed when the access changes or the step is shown again", async () => {
      const { rerender } = renderCreateStep();
      await expectFormState({ value: "true", isDirty: false });

      fireEvent.click(screen.getByRole("checkbox"));
      await expectFormState({ value: "false", isDirty: true });

      // The access value changes, for example after a read returns again.
      mockAccess("noPermission");
      rerender();
      mockAccess("editable");
      rerender();
      await expectFormState({ value: "false", isDirty: true });

      // The user goes back to "Select streams" and returns to "Configure connection".
      rerender(false);
      rerender(true);
      expect(screen.getByRole("checkbox")).not.toBeChecked();
      await expectFormState({ value: "false", isDirty: true });
    });

    it("locks the switch off and says an organization admin can turn it on when the organization setting is off", async () => {
      mockAccess("orgOff");

      renderCreateStep();

      const control = screen.getByRole("checkbox");
      expect(control).toBeDisabled();
      expect(control).not.toBeChecked();
      await expectFormState({ value: "false", isDirty: false });
      fireEvent.mouseOver(control);
      expect(
        screen.getByText(
          "Semantic search is off for this organization. An organization admin can turn it on in Context layer settings."
        )
      ).toBeVisible();
      expect(screen.queryByRole("link")).not.toBeInTheDocument();
    });

    it("locks the switch off with a tooltip when the viewer cannot edit", async () => {
      mockAccess("noPermission");

      renderCreateStep();

      const control = screen.getByRole("checkbox");
      expect(control).toBeDisabled();
      expect(control).not.toBeChecked();
      await expectFormState({ value: "false", isDirty: false });
      fireEvent.mouseOver(control);
      expect(screen.getByText("You need edit permission for this workspace to change semantic search.")).toBeVisible();
    });
  });
});
