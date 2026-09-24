import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { FormProvider, useForm } from "react-hook-form";

import { render } from "test-utils";

import { Controls } from "./Controls";

const ControlsWithForm = ({
  onSaveDraft,
  isEditMode = false,
  isDraftMode = false,
  dirty = true,
  isValid = false,
  errorMessage,
}: {
  onSaveDraft?: () => Promise<void>;
  isEditMode?: boolean;
  isDraftMode?: boolean;
  dirty?: boolean;
  isValid?: boolean;
  errorMessage?: React.ReactNode;
}) => {
  const form = useForm();
  return (
    <FormProvider {...form}>
      <Controls
        formType="source"
        isSubmitting={false}
        isValid={isValid}
        dirty={dirty}
        onCancelClick={jest.fn()}
        onRetestClick={jest.fn()}
        onCancelTesting={jest.fn()}
        connectionTestSuccess={false}
        hasDefinition
        isEditMode={isEditMode}
        isDraftMode={isDraftMode}
        onSaveDraft={onSaveDraft}
        errorMessage={errorMessage}
      />
    </FormProvider>
  );
};

describe("Controls", () => {
  it("saves an incomplete draft without running required-field validation", async () => {
    const onSaveDraft = jest.fn().mockResolvedValue(undefined);
    await render(<ControlsWithForm onSaveDraft={onSaveDraft} />);

    await userEvent.click(screen.getByRole("button", { name: "Save draft" }));

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

    expect(screen.getByRole("button", { name: "Set up source" })).toBeEnabled();
  });

  it("shows a draft check failure without offering Retest", async () => {
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
    expect(screen.queryByRole("button", { name: "Retest saved source" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Set up source" })).toBeEnabled();
  });

  it("keeps Retest available for an unchanged ready actor", async () => {
    await render(<ControlsWithForm isEditMode dirty={false} isValid />);

    expect(screen.getByRole("button", { name: "Retest saved source" })).toBeEnabled();
  });
});
