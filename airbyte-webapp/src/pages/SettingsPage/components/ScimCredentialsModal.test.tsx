import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { ScimIdpProvider } from "core/api/types/AirbyteClient";

import { ScimCredentialsModal, ScimCredentialsModalProps } from "./ScimCredentialsModal";

const SCIM_BASE_URL = "https://cloud.airbyte.com/api/public/v1/scim/v2";
const TOKEN = "airbyte_scim_4f8a2c9e7b1d4a6f8c3e5b7a9d1f3c5e9db1";
const TRUNCATED_TOKEN = "airbyte_scim_4f8a2c…9db1";

const renderModal = async (props: Partial<ScimCredentialsModalProps> = {}) => {
  const onComplete = jest.fn();
  const renderResult = await render(
    <ScimCredentialsModal
      scimBaseUrl={SCIM_BASE_URL}
      token={TOKEN}
      idpProvider={ScimIdpProvider.okta}
      onComplete={onComplete}
      {...props}
    />
  );
  return { onComplete, ...renderResult };
};

const clickCopyButtonIn = async (testId: string) => {
  const field = screen.getByTestId(testId);
  await userEvent.click(within(field).getByTestId("copy-button"));
};

describe("ScimCredentialsModal", () => {
  beforeEach(() => {
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText: jest.fn().mockResolvedValue(undefined) },
      writable: true,
      configurable: true,
    });
  });

  it("displays the token middle-truncated but copies the full value", async () => {
    await renderModal();

    expect(screen.getByTestId("bearer-token-value")).toHaveTextContent(TRUNCATED_TOKEN);

    await clickCopyButtonIn("bearer-token-field");

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith(TOKEN);
  });

  it("never puts the untruncated token in the DOM while it is truncated", async () => {
    await renderModal();

    // The base URL keeps its `title`; the token must not have one, or the full secret would sit in
    // the DOM for session-replay tooling to capture despite the visual truncation.
    expect(screen.getByTitle(SCIM_BASE_URL)).toBeInTheDocument();
    expect(screen.queryByTitle(TOKEN)).not.toBeInTheDocument();
  });

  it("gives the two copy buttons distinct accessible names", async () => {
    await renderModal();

    // Both buttons are icon-only, so without explicit titles they would both announce as "Copy" -
    // and only one of them unlocks the modal's single exit.
    expect(screen.getByRole("button", { name: "Copy SCIM base URL" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Copy bearer token" })).toBeInTheDocument();
  });

  it("keeps the CTA disabled until the bearer token has been copied", async () => {
    await renderModal();

    const cta = screen.getByRole("button", { name: "Copied and done" });
    expect(cta).toBeDisabled();

    // Copying the base URL alone must not unlock the CTA - the URL is recoverable, the token isn't.
    await clickCopyButtonIn("scim-base-url-field");
    expect(cta).toBeDisabled();

    await clickCopyButtonIn("bearer-token-field");
    expect(cta).toBeEnabled();
  });

  it("reveals the full token and unlocks the CTA when the clipboard write fails", async () => {
    (navigator.clipboard.writeText as jest.Mock).mockRejectedValue(new Error("clipboard blocked"));
    await renderModal();

    const cta = screen.getByRole("button", { name: "Copied and done" });

    // A failed base-URL copy is not a trap - the URL is fully visible - so it must not unlock the CTA.
    await clickCopyButtonIn("scim-base-url-field");
    expect(cta).toBeDisabled();
    expect(screen.getByTestId("bearer-token-value")).toHaveTextContent(TRUNCATED_TOKEN);

    await clickCopyButtonIn("bearer-token-field");

    expect(screen.getByTestId("bearer-token-value")).toHaveTextContent(TOKEN);
    expect(
      screen.getByText("Copying to the clipboard failed. Select the token and copy it manually.")
    ).toBeInTheDocument();
    expect(cta).toBeEnabled();
  });

  it("reveals the full token and unlocks the CTA when the clipboard API is unavailable", async () => {
    // A non-secure context (a self-managed instance served over plain HTTP) has no
    // `navigator.clipboard` at all, which fails differently from a rejected write: reading
    // `.writeText` off it throws synchronously, before there's a promise to reject.
    Object.defineProperty(navigator, "clipboard", { value: undefined, writable: true, configurable: true });
    await renderModal();

    const cta = screen.getByRole("button", { name: "Copied and done" });

    await clickCopyButtonIn("bearer-token-field");

    expect(screen.getByTestId("bearer-token-value")).toHaveTextContent(TOKEN);
    expect(
      screen.getByText("Copying to the clipboard failed. Select the token and copy it manually.")
    ).toBeInTheDocument();
    expect(cta).toBeEnabled();
  });

  it("calls onComplete when the CTA is clicked after copying the token", async () => {
    const { onComplete } = await renderModal();

    await clickCopyButtonIn("bearer-token-field");
    await userEvent.click(screen.getByRole("button", { name: "Copied and done" }));

    expect(onComplete).toHaveBeenCalledTimes(1);
  });

  it("renders the Okta password-sync note only when the provider is Okta", async () => {
    await renderModal({ idpProvider: ScimIdpProvider.okta });
    expect(screen.getByText("In Okta, turn off password sync. Airbyte ignores passwords.")).toBeInTheDocument();
  });

  it("does not render the Okta password-sync note for Microsoft Entra ID", async () => {
    await renderModal({ idpProvider: ScimIdpProvider.microsoft_entra_id });
    expect(screen.queryByText("In Okta, turn off password sync. Airbyte ignores passwords.")).not.toBeInTheDocument();
  });
});
