import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

import { useSupportAgentService } from "cloud/services/supportAgent";

/**
 * A reusable, icon-only button that invokes the Support Agent bot.
 *
 * This button displays a chat icon and opens the Support Agent chat panel when clicked.
 * It includes a tooltip to clarify its purpose. The button can be reused across the UI
 * wherever support access is needed, providing a consistent way for users to get help.
 *
 * NOTE: This component must only be rendered within `SupportAgentServiceProvider`.
 * Parent components should check the `SupportAgentBot` feature flag before rendering.
 */
export const SupportAgentButton: React.FC = () => {
  const { openSupportBot } = useSupportAgentService();

  return (
    <Tooltip
      placement="bottom"
      control={<Button variant="magic" size="xs" icon="chat" iconSize="md" type="button" onClick={openSupportBot} />}
    >
      <FormattedMessage id="connectorBuilder.supportAgent.tooltip" />
    </Tooltip>
  );
};
