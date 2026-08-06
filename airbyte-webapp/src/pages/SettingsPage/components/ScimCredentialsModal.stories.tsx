import { Meta, StoryObj } from "@storybook/react";

import { Modal } from "components/ui/Modal";

import { ScimIdpProvider } from "core/api/types/AirbyteClient";

import { ScimCredentialsModal } from "./ScimCredentialsModal";

const meta: Meta<typeof ScimCredentialsModal> = {
  title: "Settings/ScimCredentialsModal",
  component: ScimCredentialsModal,
  args: {
    scimBaseUrl: "https://cloud.airbyte.com/api/public/v1/scim/v2",
    token: "airbyte_scim_4f8a2c9e7b1d4a6f8c3e5b7a9d1f3c5e9db1",
    onComplete: () => undefined,
  },
  argTypes: {
    onComplete: { table: { disable: true } },
  },
  decorators: [
    (Story) => (
      <Modal size="md" title="Copy your SCIM details">
        <Story />
      </Modal>
    ),
  ],
};

export default meta;

/**
 * Okta variant - includes the password-sync info banner.
 */
export const Okta: StoryObj<typeof ScimCredentialsModal> = {
  args: {
    idpProvider: ScimIdpProvider.okta,
  },
};

/**
 * Microsoft Entra ID variant - no password-sync note, since Entra's SCIM provisioning has no
 * equivalent password-push toggle.
 */
export const MicrosoftEntraId: StoryObj<typeof ScimCredentialsModal> = {
  args: {
    idpProvider: ScimIdpProvider.microsoft_entra_id,
  },
};
