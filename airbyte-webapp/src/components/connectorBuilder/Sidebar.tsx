import classnames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";

import { AdminWorkspaceWarning } from "components/ui/AdminWorkspaceWarning";
import { FlexContainer } from "components/ui/Flex";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { FeatureItem, IfFeatureEnabled } from "core/services/features";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BaseConnectorInfo } from "./BaseConnectorInfo";
import { NameInput } from "./NameInput";
import { SavingIndicator } from "./SavingIndicator";
import styles from "./Sidebar.module.scss";
import { useBuilderWatch } from "./types";
import { UiYamlToggleButton } from "./UiYamlToggleButton";

interface SidebarProps {
  className?: string;
  yamlSelected: boolean;
}

export const Sidebar: React.FC<React.PropsWithChildren<SidebarProps>> = ({ className, yamlSelected, children }) => {
  const analyticsService = useAnalyticsService();
  const { toggleUI, isResolving, currentProject } = useConnectorBuilderFormState();
  const formValues = useBuilderWatch("formValues");
  const showSavingIndicator = yamlSelected || formValues.streams.length > 0;

  const OnUiToggleClick = () => {
    toggleUI(yamlSelected ? "ui" : "yaml");
    analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.TOGGLE_UI_YAML, {
      actionDescription: "User clicked the UI | YAML toggle button",
      current_view: yamlSelected ? "yaml" : "ui",
      new_view: yamlSelected ? "ui" : "yaml",
    });
  };

  return (
    <FlexContainer direction="column" alignItems="stretch" gap="lg" className={classnames(className, styles.container)}>
      <IfFeatureEnabled feature={FeatureItem.ShowAdminWarningInWorkspace}>
        <AdminWorkspaceWarning />
      </IfFeatureEnabled>
      <UiYamlToggleButton
        yamlSelected={yamlSelected}
        onClick={OnUiToggleClick}
        size="sm"
        disabled={yamlSelected && isResolving}
        tooltip={
          yamlSelected && isResolving ? <FormattedMessage id="connectorBuilder.resolvingStreamList" /> : undefined
        }
      />

      <FlexContainer direction="column" alignItems="center" className={styles.contained}>
        <FlexContainer direction="column" gap="none" className={styles.contained}>
          <NameInput className={styles.connectorName} size="md" />
          {currentProject.baseActorDefinitionVersionInfo && (
            <BaseConnectorInfo {...currentProject.baseActorDefinitionVersionInfo} showDocsLink />
          )}
        </FlexContainer>

        {showSavingIndicator && <SavingIndicator />}
      </FlexContainer>

      <FlexContainer direction="column" alignItems="stretch" gap="xl" className={styles.modeSpecificContent}>
        {children}
      </FlexContainer>
    </FlexContainer>
  );
};
