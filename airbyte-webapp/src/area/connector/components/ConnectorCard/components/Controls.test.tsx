import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { FormProvider, useForm } from "react-hook-form";

import { Form } from "components/ui/forms";
import { render } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { FormFooterSlotContext } from "area/connector/components/ConnectorDocumentationLayout/FormFooterSlotContext";

import { Controls } from "./Controls";

const FILL_REQUIRED_FIELDS = "Fill out required fields before saving";
const FORM_ELEMENT_ID = "connector-form";

const ControlsWithForm = ({
  onSaveDraft,
  onSubmitWithoutCheck,
  isEditMode = false,
  isDraftMode = false,
  dirty = true,
  isValid = false,
  emptyRequiredFields = [],
  errorMessage,
}: {
  onSaveDraft?: () => Promise<void>;
  onSubmitWithoutCheck?: () => Promise<void>;
  isEditMode?: boolean;
  isDraftMode?: boolean;
  dirty?: boolean;
  isValid?: boolean;
  emptyRequiredFields?: string[];
  errorMessage?: React.ReactNode;
}) => {
  const form = useForm();
  return (
    <FormProvider {...form}>
      <Controls
        formType="source"
        isSubmitting={false}
        isValid={isValid}
        emptyRequiredFields={emptyRequiredFields}
        dirty={dirty}
        onCancelClick={jest.fn()}
        onRetestClick={jest.fn()}
        onCancelTesting={jest.fn()}
        connectionTestSuccess={false}
        hasDefinition
        isEditMode={isEditMode}
        isDraftMode={isDraftMode}
        onSaveDraft={onSaveDraft}
        onSubmitWithoutCheck={onSubmitWithoutCheck}
        errorMessage={errorMessage}
        formElementId={FORM_ELEMENT_ID}
        canEdit
      />
    </FormProvider>
  );
};

// A disabled button gets no pointer events, so hover the tooltip trigger that wraps it
const hoverTooltipTrigger = async (button: HTMLElement) => {
  const trigger = button.closest("span");
  if (!trigger) {
    throw new Error("No tooltip trigger wraps the button");
  }
  await userEvent.hover(trigger);
};

const ControlsInForm = ({
  onSubmit,
  withFooterSlot,
  canEdit = true,
  isEditMode = false,
  onSubmitWithoutCheck,
}: {
  onSubmit: () => Promise<void>;
  withFooterSlot: boolean;
  canEdit?: boolean;
  isEditMode?: boolean;
  onSubmitWithoutCheck?: () => Promise<void>;
}) => {
  const [footerSlot, setFooterSlot] = useState<HTMLDivElement | null>(null);
  return (
    <FormFooterSlotContext.Provider value={withFooterSlot ? footerSlot : null}>
      <Form id={FORM_ELEMENT_ID} defaultValues={{}} onSubmit={onSubmit} disabled={!canEdit} dataTestId="form">
        <Controls
          formType="source"
          isSubmitting={false}
          isValid
          emptyRequiredFields={[]}
          dirty
          onCancelClick={jest.fn()}
          onDeleteClick={jest.fn()}
          onRetestClick={jest.fn()}
          onCancelTesting={jest.fn()}
          connectionTestSuccess={false}
          hasDefinition
          isEditMode={isEditMode}
          onSubmitWithoutCheck={onSubmitWithoutCheck}
          formElementId={FORM_ELEMENT_ID}
          canEdit={canEdit}
        />
      </Form>
      <div ref={setFooterSlot} data-testid="footer-slot" />
    </FormFooterSlotContext.Provider>
  );
};

describe("Controls", () => {
  beforeEach(() => {
    mockExperiments({ "connector.allowSavingWithoutTesting": false });
    jest.mocked(Element.prototype.scrollIntoView).mockClear();
  });

  it("saves an incomplete draft without running required-field validation", async () => {
    const onSaveDraft = jest.fn().mockResolvedValue(undefined);
    await render(<ControlsWithForm onSaveDraft={onSaveDraft} emptyRequiredFields={["Host"]} />);

    await userEvent.click(screen.getByRole("button", { name: "Save as a draft" }));

    expect(onSaveDraft).toHaveBeenCalledTimes(1);
  });

  it("allows an unchanged valid edit-mode draft to be set up", async () => {
    await render(
      <ControlsWithForm
        onSaveDraft={jest.fn().mockResolvedValue(undefined)}
        isEditMode
        isDraftMode
        dirty={false}
        isValid
      />
    );

    expect(screen.getByRole("button", { name: "Save & Test" })).toBeEnabled();
  });

  it("shows a draft check failure without offering Retest, and scrolls the check card into view", async () => {
    const scrollIntoView = jest.mocked(Element.prototype.scrollIntoView);
    await render(
      <ControlsWithForm
        onSaveDraft={jest.fn().mockResolvedValue(undefined)}
        isEditMode
        isDraftMode
        dirty={false}
        isValid
        errorMessage="check failed"
      />
    );

    expect(screen.getByText("check failed")).toBeInTheDocument();
    expect(scrollIntoView).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole("button", { name: "Retest saved source" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Save & Test" })).toBeEnabled();
  });

  it("disables Save & Test with a tooltip that lists the empty required fields until they are filled", async () => {
    const { rerender } = await render(
      <ControlsWithForm onSaveDraft={jest.fn().mockResolvedValue(undefined)} emptyRequiredFields={["Host", "Port"]} />
    );

    const saveAndTest = screen.getByRole("button", { name: "Save & Test" });
    expect(saveAndTest).toBeDisabled();
    expect(saveAndTest).toHaveAccessibleDescription(`${FILL_REQUIRED_FIELDS}: Host and Port`);
    expect(screen.getByRole("button", { name: "Save as a draft" })).toBeEnabled();

    await hoverTooltipTrigger(saveAndTest);
    const tooltip = await screen.findByRole("tooltip");
    expect(tooltip).toHaveTextContent(FILL_REQUIRED_FIELDS);
    expect(
      within(tooltip)
        .getAllByRole("listitem")
        .map((item) => item.textContent)
    ).toEqual(["Host", "Port"]);

    rerender(<ControlsWithForm onSaveDraft={jest.fn().mockResolvedValue(undefined)} />);

    expect(screen.getByRole("button", { name: "Save & Test" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Save & Test" })).not.toHaveAccessibleDescription();
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("lists the first three empty required fields and counts the rest", async () => {
    await render(<ControlsWithForm emptyRequiredFields={["Host", "Port", "Database", "Username", "Password"]} />);

    const saveAndTest = screen.getByRole("button", { name: "Save & Test" });
    expect(saveAndTest).toHaveAccessibleDescription(`${FILL_REQUIRED_FIELDS}: Host, Port, Database, and 2 more fields`);

    await hoverTooltipTrigger(saveAndTest);
    const tooltip = await screen.findByRole("tooltip");
    expect(
      within(tooltip)
        .getAllByRole("listitem")
        .map((item) => item.textContent)
    ).toEqual(["Host", "Port", "Database", "2 more fields"]);
  });

  it("disables Save & Test in edit mode while a required field is empty", async () => {
    await render(<ControlsWithForm isEditMode emptyRequiredFields={["Host"]} />);

    const saveAndTest = screen.getByRole("button", { name: "Save & Test" });
    expect(saveAndTest).toBeDisabled();
    expect(saveAndTest).toHaveAccessibleDescription(`${FILL_REQUIRED_FIELDS}: Host`);
  });

  it("disables Save & Test, but not Save as a draft, in an edit-mode draft while a required field is empty", async () => {
    await render(
      <ControlsWithForm
        onSaveDraft={jest.fn().mockResolvedValue(undefined)}
        isEditMode
        isDraftMode
        dirty={false}
        emptyRequiredFields={["Host"]}
      />
    );

    const saveAndTest = screen.getByRole("button", { name: "Save & Test" });
    expect(saveAndTest).toBeDisabled();
    expect(saveAndTest).toHaveAccessibleDescription(`${FILL_REQUIRED_FIELDS}: Host`);
    expect(screen.getByRole("button", { name: "Save as a draft" })).toBeEnabled();
  });

  it("disables the Save & Test dropdown and its caret with a tooltip until the required fields are filled", async () => {
    mockExperiments({ "connector.allowSavingWithoutTesting": true });
    const onSubmitWithoutCheck = jest.fn().mockResolvedValue(undefined);
    const { rerender } = await render(
      <ControlsWithForm onSubmitWithoutCheck={onSubmitWithoutCheck} emptyRequiredFields={["Host"]} />
    );

    const saveAndTest = screen.getByRole("button", { name: "Save & Test" });
    expect(saveAndTest).toBeDisabled();
    expect(saveAndTest).toHaveAccessibleDescription(`${FILL_REQUIRED_FIELDS}: Host`);
    expect(screen.getByTestId("create-source-button_dropdown-caret")).toBeDisabled();

    await hoverTooltipTrigger(saveAndTest);
    expect(within(await screen.findByRole("tooltip")).getByRole("listitem")).toHaveTextContent(/^Host$/);

    rerender(<ControlsWithForm onSubmitWithoutCheck={onSubmitWithoutCheck} />);
    await userEvent.click(screen.getByTestId("create-source-button_dropdown-caret"));
    await userEvent.click(await screen.findByText("Save without testing"));

    await waitFor(() => expect(onSubmitWithoutCheck).toHaveBeenCalledTimes(1));
  });

  it("disables an unchanged edit without the required-fields tooltip", async () => {
    await render(<ControlsWithForm isEditMode dirty={false} isValid />);

    const saveAndTest = screen.getByRole("button", { name: "Save & Test" });
    expect(saveAndTest).toBeDisabled();
    expect(saveAndTest).not.toHaveAccessibleDescription();

    await hoverTooltipTrigger(saveAndTest);
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("keeps Retest available for an unchanged ready actor, without scrolling to the check card", async () => {
    const scrollIntoView = jest.mocked(Element.prototype.scrollIntoView);
    await render(<ControlsWithForm isEditMode dirty={false} isValid />);

    expect(screen.getByRole("button", { name: "Retest saved source" })).toBeEnabled();
    expect(scrollIntoView).not.toHaveBeenCalled();
  });

  describe("footer slot", () => {
    it("submits the form from the footer slot outside the form element", async () => {
      const onSubmit = jest.fn().mockResolvedValue(undefined);
      await render(<ControlsInForm onSubmit={onSubmit} withFooterSlot />);

      const submitButton = screen.getByRole("button", { name: "Save & Test" });
      expect(screen.getByTestId("footer-slot")).toContainElement(submitButton);
      expect(screen.getByTestId("form")).not.toContainElement(submitButton);

      await userEvent.click(submitButton);

      expect(onSubmit).toHaveBeenCalledTimes(1);
    });

    it("disables every button in the footer slot when the user cannot edit the connector", async () => {
      const onSubmit = jest.fn().mockResolvedValue(undefined);
      await render(<ControlsInForm onSubmit={onSubmit} withFooterSlot isEditMode canEdit={false} />);

      const footerSlot = screen.getByTestId("footer-slot");
      for (const name of ["Delete this source", "Cancel", "Save & Test"]) {
        expect(within(footerSlot).getByRole("button", { name })).toBeDisabled();
      }

      await userEvent.click(within(footerSlot).getByRole("button", { name: "Save & Test" }));

      expect(onSubmit).not.toHaveBeenCalled();
    });

    it("submits the form from the Save & Test dropdown in the footer slot, and disables it with its caret when the user cannot edit", async () => {
      mockExperiments({ "connector.allowSavingWithoutTesting": true });
      const onSubmit = jest.fn().mockResolvedValue(undefined);
      const onSubmitWithoutCheck = jest.fn().mockResolvedValue(undefined);
      const { rerender } = await render(
        <ControlsInForm onSubmit={onSubmit} onSubmitWithoutCheck={onSubmitWithoutCheck} withFooterSlot />
      );

      const submitButton = screen.getByRole("button", { name: "Save & Test" });
      expect(screen.getByTestId("footer-slot")).toContainElement(submitButton);

      await userEvent.click(submitButton);

      expect(onSubmit).toHaveBeenCalledTimes(1);

      rerender(
        <ControlsInForm
          onSubmit={onSubmit}
          onSubmitWithoutCheck={onSubmitWithoutCheck}
          withFooterSlot
          canEdit={false}
        />
      );

      expect(screen.getByRole("button", { name: "Save & Test" })).toBeDisabled();
      expect(screen.getByTestId("create-source-button_dropdown-caret")).toBeDisabled();
    });

    it("renders the buttons in the form and submits it when there is no footer slot", async () => {
      const onSubmit = jest.fn().mockResolvedValue(undefined);
      await render(<ControlsInForm onSubmit={onSubmit} withFooterSlot={false} />);

      const submitButton = screen.getByRole("button", { name: "Save & Test" });
      expect(screen.getByTestId("form")).toContainElement(submitButton);

      await userEvent.click(submitButton);

      expect(onSubmit).toHaveBeenCalledTimes(1);
    });
  });
});
