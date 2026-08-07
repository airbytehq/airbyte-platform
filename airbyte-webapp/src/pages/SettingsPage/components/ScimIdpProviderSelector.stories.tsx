import { Meta, StoryObj } from "@storybook/react";
import { useState } from "react";

import { ScimIdpProvider } from "core/api/types/AirbyteClient";

import { ScimIdpProviderSelector } from "./ScimIdpProviderSelector";

const meta: Meta<typeof ScimIdpProviderSelector> = {
  title: "Settings/ScimIdpProviderSelector",
  component: ScimIdpProviderSelector,
  argTypes: {
    value: {
      table: { disable: true },
    },
    onChange: {
      table: { disable: true },
    },
  },
  decorators: [
    (Story) => (
      <div style={{ maxWidth: 320 }}>
        <Story />
      </div>
    ),
  ],
};

export default meta;

const ScimIdpProviderSelectorExample = () => {
  const [value, setValue] = useState<ScimIdpProvider | undefined>(undefined);
  return <ScimIdpProviderSelector value={value} onChange={setValue} />;
};

/**
 * Controlled selector - clicking a segment actually moves the selection.
 */
export const Interactive: StoryObj<typeof ScimIdpProviderSelector> = {
  render: () => <ScimIdpProviderSelectorExample />,
};

/**
 * No `value` provided - the valid initial "not configured yet" state. Neither segment is
 * selected; no vendor is ever pre-selected for the user.
 */
export const NoSelection: StoryObj<typeof ScimIdpProviderSelector> = {
  args: {
    value: undefined,
    onChange: () => undefined,
  },
};

/**
 * `disabled` with a stored `value` - SCIM has been configured for this provider and then
 * disabled. The control greys out but keeps the stored provider's highlight (lightened, not
 * dropped) so the admin can still see which vendor is on file.
 *
 * There is intentionally no "inert + no selection" story: the backend always retains
 * `idpProvider` once SCIM has been enabled, so a disabled control with no stored selection is not
 * a reachable product state.
 */
export const InertWithStoredSelection: StoryObj<typeof ScimIdpProviderSelector> = {
  args: {
    value: ScimIdpProvider.okta,
    onChange: () => undefined,
    disabled: true,
  },
};
