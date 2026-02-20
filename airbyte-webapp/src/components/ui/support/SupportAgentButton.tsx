import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentConnectionIdOptional } from "area/connection/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useFeature, FeatureItem } from "core/services/features";

import { SupportChatPanel } from "./SupportAgentWidget";
import { SupportChatPanelPortal } from "./SupportChatPanelPortal";
import { useSupportChatPanelState } from "./useSupportChatPanelState";

/**
 * A reusable, icon-only button that invokes the Support Agent bot.
 *
 * This button displays a chat icon and opens the Support Agent chat panel when clicked.
 * It includes a tooltip to clarify its purpose. The button can be reused across the UI
 * wherever support access is needed, providing a consistent way for users to get help.
 *
 * The button will only render if the SupportAgentBot feature is enabled and a valid
 * workspace context exists.
 */
export const SupportAgentButton: React.FC = () => {
  const supportEnabled = useFeature(FeatureItem.SupportAgentBot);
  const workspaceId = useCurrentWorkspaceId();
  const connectionId = useCurrentConnectionIdOptional();
  const { isOpen, setIsOpen, isExpanded, setIsExpanded, hasBeenOpened, setHasBeenOpened } = useSupportChatPanelState();

  // Don't render if feature disabled or no workspace context
  if (!supportEnabled || !workspaceId) {
    return null;
  }

  return (
    <>
      <Tooltip
        placement="bottom"
        control={
          <Button
            variant="magic"
            size="xs"
            icon="chat"
            iconSize="md"
            type="button"
            onClick={() => {
              setHasBeenOpened(true);
              setIsOpen(true);
            }}
          />
        }
      >
        <FormattedMessage id="connectorBuilder.supportAgent.tooltip" />
      </Tooltip>
      {hasBeenOpened && (
        <SupportChatPanelPortal isOpen={isOpen}>
          <SupportChatPanel
            workspaceId={workspaceId}
            connectionId={connectionId}
            isExpanded={isExpanded}
            setIsExpanded={setIsExpanded}
            onClose={() => setIsOpen(false)}
          />
        </SupportChatPanelPortal>
      )}
    </>
  );
};
