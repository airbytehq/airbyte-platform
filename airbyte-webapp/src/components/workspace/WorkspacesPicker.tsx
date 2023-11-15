import { autoUpdate, offset, useFloating } from "@floating-ui/react-dom";
import { Popover } from "@headlessui/react";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useCurrentWorkspace } from "core/api";

import styles from "./WorkspacesPicker.module.scss";
import { WorkspaceFetcher, WorkspacesPickerList } from "./WorkspacesPickerList";

const WorkspaceButton = React.forwardRef<HTMLButtonElement | null, React.ButtonHTMLAttributes<HTMLButtonElement>>(
  ({ children, ...props }, ref) => {
    return (
      <button ref={ref} className={styles.workspacesPicker__button} {...props}>
        {children}
      </button>
    );
  }
);

WorkspaceButton.displayName = "WorkspaceButton";

export const WorkspacesPicker: React.FC<{ useFetchWorkspaces: WorkspaceFetcher }> = ({ useFetchWorkspaces }) => {
  const currentWorkspace = useCurrentWorkspace();

  const { x, y, reference, floating, strategy } = useFloating({
    middleware: [offset(10)],
    whileElementsMounted: autoUpdate,
    placement: "right-start",
  });

  return (
    <Popover className={styles.workspacesPicker__container}>
      {({ close }) => (
        <>
          <Popover.Button ref={reference} as={WorkspaceButton}>
            <span className={styles.workspacesPicker__buttonContent}>
              <Text color="grey" bold className={styles.workspacesPicker__buttonText} align="center">
                {currentWorkspace.name}
              </Text>
              <Icon type="chevronDown" color="disabled" />
            </span>
          </Popover.Button>
          <Popover.Panel
            ref={floating}
            style={{
              position: strategy,
              top: y ?? 0,
              left: x ?? 0,
            }}
          >
            <div className={styles.workspacesPicker__popoverPanel}>
              <Box p="md">
                <Box pb="xs">
                  <Text color="grey" size="sm" bold align="center">
                    <FormattedMessage id="workspaces.workspace" />
                  </Text>
                </Box>
                <Text size="lg" bold align="center">
                  {currentWorkspace.name}
                </Text>
              </Box>
              <WorkspacesPickerList useFetchWorkspaces={useFetchWorkspaces} closePicker={close} />
            </div>
          </Popover.Panel>
        </>
      )}
    </Popover>
  );
};
