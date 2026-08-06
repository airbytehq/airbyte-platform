import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { useScimSettingsAccess } from "area/organization/utils";
import { useEnableScim } from "core/api";
import { ScimConfigResponse, ScimConfigStatus, ScimIdpProvider } from "core/api/types/AirbyteClient";
import { useModalService } from "core/services/Modal";
import { useNotificationService } from "core/services/Notification";

import { ScimSettingsCard } from "./ScimSettingsCard";

// core/api's import graph is circular, so a jest.requireActual spread fails at module evaluation
// (same rationale as SSOAndScimOrganizationSettingsPage.test.tsx) - list its exports explicitly.
// area/organization/utils follows the same explicit-factory style for consistency with that file.
jest.mock("area/organization/utils", () => ({
  useScimSettingsAccess: jest.fn(),
}));

jest.mock("core/api", () => ({
  useEnableScim: jest.fn(),
}));

// Modal/Notification are not circular, so spread the real module - `render` (test-utils) mounts the
// *real* ModalServiceProvider/NotificationService from TestWrapper, and only the hook this
// component calls is swapped out; replacing the whole module would blank out those providers too.
jest.mock("core/services/Modal", () => ({
  ...jest.requireActual("core/services/Modal"),
  useModalService: jest.fn(),
}));

jest.mock("core/services/Notification", () => ({
  ...jest.requireActual("core/services/Notification"),
  useNotificationService: jest.fn(),
}));

const mockUseScimSettingsAccess = useScimSettingsAccess as jest.Mock;
const mockUseEnableScim = useEnableScim as jest.Mock;
const mockUseModalService = useModalService as jest.Mock;
const mockUseNotificationService = useNotificationService as jest.Mock;

const SCIM_BASE_URL = "https://cloud.airbyte.com/api/public/v1/scim/v2";
const TOKEN = "airbyte_scim_4f8a2c9e7b1d4a6f8c3e5b7a9d1f3c5e9db1";

const baseConfig: ScimConfigResponse = {
  status: ScimConfigStatus.not_configured,
  scimBaseUrl: SCIM_BASE_URL,
  available: true,
};

const setAccess = (overrides: {
  canManageScim?: boolean;
  isScimAvailable?: boolean;
  scimConfig?: ScimConfigResponse;
  isLoading?: boolean;
  isError?: boolean;
}) => {
  mockUseScimSettingsAccess.mockReturnValue({
    canManageScim: true,
    isScimAvailable: true,
    scimConfig: baseConfig,
    isLoading: false,
    isError: false,
    ...overrides,
  });
};

// CollapsibleSettingsCard only mounts `children` once expanded (see SSOSettingsValidation.test.tsx's
// "keeps the card closed" case for the same behavior on the sibling SSO card) - the header itself
// (label + status icon) is always rendered, but the body (chip, description, selector, button) is
// not. Every test that inspects the body must open the disclosure first.
const renderOpenCard = async () => {
  const renderResult = await render(<ScimSettingsCard />);
  await userEvent.click(screen.getByRole("button", { name: "SCIM" }));
  return renderResult;
};

describe("ScimSettingsCard", () => {
  let mockMutateAsync: jest.Mock;
  let mockReset: jest.Mock;
  let mockOpenModal: jest.Mock;
  let mockRegisterNotification: jest.Mock;

  beforeEach(() => {
    jest.clearAllMocks();

    Object.defineProperty(navigator, "clipboard", {
      value: { writeText: jest.fn().mockResolvedValue(undefined) },
      writable: true,
      configurable: true,
    });

    mockMutateAsync = jest.fn();
    mockReset = jest.fn();
    mockUseEnableScim.mockReturnValue({ mutateAsync: mockMutateAsync, isLoading: false, reset: mockReset });

    mockOpenModal = jest.fn();
    mockUseModalService.mockReturnValue({ openModal: mockOpenModal, getCurrentModalTitle: jest.fn() });

    mockRegisterNotification = jest.fn();
    mockUseNotificationService.mockReturnValue({
      registerNotification: mockRegisterNotification,
      unregisterNotificationById: jest.fn(),
    });

    setAccess({});
  });

  describe("visibility gating", () => {
    it("renders nothing when isScimAvailable is false", async () => {
      setAccess({ isScimAvailable: false, scimConfig: { ...baseConfig, available: false } });

      await render(<ScimSettingsCard />);

      expect(screen.queryByText("SCIM")).not.toBeInTheDocument();
      expect(screen.queryByRole("button")).not.toBeInTheDocument();
    });

    it("renders nothing while the config query is loading, even if a config is already cached", async () => {
      setAccess({ isLoading: true, scimConfig: baseConfig, isScimAvailable: true });

      await render(<ScimSettingsCard />);

      expect(screen.queryByText("SCIM")).not.toBeInTheDocument();
    });

    it("renders nothing when the access hook yields no config (e.g. not an org admin)", async () => {
      setAccess({ canManageScim: false, scimConfig: undefined, isScimAvailable: false });

      await render(<ScimSettingsCard />);

      expect(screen.queryByText("SCIM")).not.toBeInTheDocument();
    });
  });

  describe("not configured", () => {
    it("renders the grey chip, selector, and footnote, with no header check icon", async () => {
      await renderOpenCard();

      expect(screen.getByText("SCIM provisioning")).toBeInTheDocument();
      expect(screen.getByText("Not configured")).toBeInTheDocument();
      expect(screen.getByRole("radiogroup", { name: "Identity provider" })).toBeInTheDocument();
      expect(
        screen.getByText("You can't change this later. To switch providers you'll need to contact support.")
      ).toBeInTheDocument();
      expect(document.querySelector('[data-icon="check"]')).not.toBeInTheDocument();
    });

    it("disables the Enable button until a provider is selected", async () => {
      await renderOpenCard();

      const enableButton = screen.getByRole("button", { name: "Enable SCIM" });
      expect(enableButton).toBeDisabled();

      await userEvent.click(screen.getByRole("radio", { name: "Okta" }));

      expect(enableButton).toBeEnabled();
    });
  });

  describe("enabling", () => {
    const selectOktaAndEnable = async () => {
      await renderOpenCard();
      await userEvent.click(screen.getByRole("radio", { name: "Okta" }));
      await userEvent.click(screen.getByRole("button", { name: "Enable SCIM" }));
    };

    it("calls mutateAsync with the selected provider, opens a non-dismissable modal seeded with the resolved token, and resets afterwards", async () => {
      const enableResponse: ScimConfigResponse = {
        ...baseConfig,
        status: ScimConfigStatus.enabled,
        idpProvider: ScimIdpProvider.okta,
        token: TOKEN,
      };
      mockMutateAsync.mockResolvedValue(enableResponse);
      mockOpenModal.mockResolvedValue({ type: "completed", reason: undefined });

      await selectOktaAndEnable();

      await waitFor(() => expect(mockMutateAsync).toHaveBeenCalledWith(ScimIdpProvider.okta));
      await waitFor(() => expect(mockOpenModal).toHaveBeenCalledTimes(1));

      const modalOptions = mockOpenModal.mock.calls[0][0];
      expect(modalOptions.preventCancel).toBe(true);
      // Without `allowNavigation`, a location change closes the modal without resolving
      // `openModal`, destroying the one-time token.
      expect(modalOptions.allowNavigation).toBe(true);
      expect(modalOptions.title).toBe("Copy your SCIM details");

      // Render the exact `content` component ScimSettingsCard handed to openModal to prove it was
      // seeded from the *response* (scimBaseUrl/token/idpProvider), not local component state.
      const ModalContent = modalOptions.content;
      await render(<ModalContent onComplete={jest.fn()} onCancel={jest.fn()} />);

      expect(screen.getByTitle(SCIM_BASE_URL)).toBeInTheDocument();
      expect(screen.getByTestId("bearer-token-value")).toHaveTextContent(TOKEN.slice(0, 19));
      expect(screen.getByText("In Okta, turn off password sync. Airbyte ignores passwords.")).toBeInTheDocument();

      await waitFor(() => expect(mockReset).toHaveBeenCalledTimes(1));
    });

    it("seeds the modal from the response's idpProvider, not the local selection, when they disagree", async () => {
      const enableResponse: ScimConfigResponse = {
        ...baseConfig,
        status: ScimConfigStatus.enabled,
        idpProvider: ScimIdpProvider.microsoft_entra_id,
        token: TOKEN,
      };
      mockMutateAsync.mockResolvedValue(enableResponse);
      mockOpenModal.mockResolvedValue({ type: "completed", reason: undefined });

      // Selection is Okta, but the response echoes Entra - the modal must follow the response.
      await selectOktaAndEnable();

      await waitFor(() => expect(mockOpenModal).toHaveBeenCalledTimes(1));
      const ModalContent = mockOpenModal.mock.calls[0][0].content;
      await render(<ModalContent onComplete={jest.fn()} onCancel={jest.fn()} />);

      expect(screen.queryByText("In Okta, turn off password sync. Airbyte ignores passwords.")).not.toBeInTheDocument();
    });

    it("does not open a modal or reset when the enable response has no token (idempotent re-submit)", async () => {
      mockMutateAsync.mockResolvedValue({ ...baseConfig, status: ScimConfigStatus.enabled });

      await selectOktaAndEnable();

      await waitFor(() => expect(mockMutateAsync).toHaveBeenCalledWith(ScimIdpProvider.okta));
      expect(mockOpenModal).not.toHaveBeenCalled();
      expect(mockReset).not.toHaveBeenCalled();
      expect(mockRegisterNotification).not.toHaveBeenCalled();
    });

    it("shows an error toast and does not open a modal when the mutation rejects", async () => {
      mockMutateAsync.mockRejectedValue(new Error("boom"));

      await selectOktaAndEnable();

      await waitFor(() =>
        expect(mockRegisterNotification).toHaveBeenCalledWith(
          expect.objectContaining({ id: "scim-enable-error", type: "error" })
        )
      );
      expect(mockOpenModal).not.toHaveBeenCalled();
      expect(mockReset).not.toHaveBeenCalled();
    });
  });

  describe("already configured", () => {
    it("renders a green chip and header check icon, with no selector or Enable button, when enabled", async () => {
      setAccess({
        scimConfig: { ...baseConfig, status: ScimConfigStatus.enabled, idpProvider: ScimIdpProvider.okta },
      });

      await renderOpenCard();

      expect(screen.getByText("Enabled")).toBeInTheDocument();
      expect(document.querySelector('[data-icon="check"]')).toBeInTheDocument();
      expect(screen.queryByRole("radiogroup")).not.toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Enable SCIM" })).not.toBeInTheDocument();
    });

    it("renders a red chip with no header icon, and no selector or Enable button, when disabled", async () => {
      setAccess({
        scimConfig: { ...baseConfig, status: ScimConfigStatus.disabled, idpProvider: ScimIdpProvider.okta },
      });

      await renderOpenCard();

      expect(screen.getByText("Disabled")).toBeInTheDocument();
      expect(document.querySelector('[data-icon="check"]')).not.toBeInTheDocument();
      expect(screen.queryByRole("radiogroup")).not.toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Enable SCIM" })).not.toBeInTheDocument();
    });
  });
});
