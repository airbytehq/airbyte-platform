import classNames from "classnames";
import { createPortal } from "react-dom";

import styles from "./SupportChatPanelPortal.module.scss";

/**
 * Portal wrapper for the Support Chat Panel.
 *
 * This component handles the positioning and visibility of the Support Chat Panel
 * by rendering it in a portal to document.body with fixed positioning. This ensures
 * the panel appears above all other content with proper z-index layering.
 *
 * Used by both SupportAgentWidget (floating button) and SupportAgentButton
 * (MenuBar integration) to provide consistent positioning.
 */
interface SupportChatPanelPortalProps {
  isOpen: boolean;
  children: React.ReactNode;
}

export const SupportChatPanelPortal: React.FC<SupportChatPanelPortalProps> = ({ isOpen, children }) => {
  return createPortal(
    <div className={classNames(styles.panelContainer, { [styles.hidden]: !isOpen })}>{children}</div>,
    document.body
  );
};
