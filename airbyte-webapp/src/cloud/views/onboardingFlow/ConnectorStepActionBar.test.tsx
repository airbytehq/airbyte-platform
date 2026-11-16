import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { TestWrapper } from "test-utils/testutils";

import { CONNECTOR_STEP_FORM_ID, ConnectorStepActionBar } from "./ConnectorStepActionBar";

const renderActionBar = ({ isConnectorReady, isDirty }: { isConnectorReady: boolean; isDirty: boolean }) => {
  const onContinue = jest.fn();
  render(
    <TestWrapper>
      <ConnectorStepActionBar
        isConnectorReady={isConnectorReady}
        canEditConnector
        formState={{ isDirty, isSubmitting: false }}
        isTestConnectionInProgress={false}
        onBack={jest.fn()}
        onContinue={onContinue}
        isSavingStatus={false}
      />
    </TestWrapper>
  );
  return { continueButton: screen.getByRole("button", { name: "Continue" }), onContinue };
};

describe(`${ConnectorStepActionBar.name}`, () => {
  it.each([
    { connector: "a draft", isConnectorReady: false, isDirty: false },
    { connector: "a changed ready connector", isConnectorReady: true, isDirty: true },
  ])("Continue submits the connector form for $connector", ({ isConnectorReady, isDirty }) => {
    const { continueButton } = renderActionBar({ isConnectorReady, isDirty });

    expect(continueButton).toBeEnabled();
    expect(continueButton).toHaveAttribute("type", "submit");
    expect(continueButton).toHaveAttribute("form", CONNECTOR_STEP_FORM_ID);
  });

  it("Continue moves on without a test for a ready connector with no changes", async () => {
    const { continueButton, onContinue } = renderActionBar({ isConnectorReady: true, isDirty: false });

    await userEvent.click(continueButton);

    expect(continueButton).toHaveAttribute("type", "button");
    expect(onContinue).toHaveBeenCalledTimes(1);
  });
});
