import classnames from "classnames";
import React from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import Indicator from "components/Indicator";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { BuilderView, useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { AddStreamButton } from "./AddStreamButton";
import styles from "./BuilderSidebar.module.scss";
import { Sidebar } from "../Sidebar";
import { useBuilderWatch } from "../types";
import { useBuilderErrors } from "../useBuilderErrors";

interface ViewSelectButtonProps {
  className?: string;
  selected: boolean;
  showErrorIndicator: boolean;
  onClick: () => void;
  "data-testid": string;
}

const ViewSelectButton: React.FC<React.PropsWithChildren<ViewSelectButtonProps>> = ({
  children,
  className,
  selected,
  showErrorIndicator,
  onClick,
  "data-testid": testId,
}) => {
  return (
    <button
      type="button"
      data-testid={testId}
      className={classnames(className, styles.viewButton, {
        [styles.selectedViewButton]: selected,
        [styles.unselectedViewButton]: !selected,
      })}
      onClick={onClick}
    >
      <div className={styles.viewLabel}>{children}</div>
      {showErrorIndicator && <Indicator className={styles.errorIndicator} />}
    </button>
  );
};

interface BuilderSidebarProps {
  className?: string;
}

export const BuilderSidebar: React.FC<BuilderSidebarProps> = () => {
  const analyticsService = useAnalyticsService();
  const { hasErrors } = useBuilderErrors();
  const { permission } = useConnectorBuilderFormState();
  const { setValue } = useFormContext();
  const formValues = useBuilderWatch("formValues");
  const view = useBuilderWatch("view");
  const handleViewSelect = (selectedView: BuilderView) => {
    setValue("view", selectedView);
  };

  return (
    <Sidebar yamlSelected={false}>
      <FlexContainer direction="column" alignItems="stretch" gap="none">
        <ViewSelectButton
          data-testid="navbutton-global"
          selected={view === "global"}
          showErrorIndicator={hasErrors(["global"])}
          onClick={() => {
            handleViewSelect("global");
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.GLOBAL_CONFIGURATION_SELECT, {
              actionDescription: "Global Configuration view selected",
            });
          }}
        >
          <Icon type="parameters" />
          <Text className={styles.streamViewText}>
            <FormattedMessage id="connectorBuilder.globalConfiguration" />
          </Text>
        </ViewSelectButton>

        <ViewSelectButton
          data-testid="navbutton-inputs"
          showErrorIndicator={false}
          selected={view === "inputs"}
          onClick={() => {
            handleViewSelect("inputs");
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.USER_INPUTS_SELECT, {
              actionDescription: "User Inputs view selected",
            });
          }}
        >
          <Icon type="user" />
          <Text className={styles.streamViewText}>
            <FormattedMessage
              id="connectorBuilder.userInputs"
              values={{
                number: formValues.inputs.length,
              }}
            />
          </Text>
        </ViewSelectButton>
      </FlexContainer>

      <FlexContainer direction="column" alignItems="stretch" gap="sm" className={styles.streamListContainer}>
        <FlexContainer className={styles.streamsHeader} alignItems="center" justifyContent="space-between">
          <FlexContainer alignItems="center" gap="none">
            <Text className={styles.streamsHeading} size="xs" bold>
              <FormattedMessage id="connectorBuilder.streamsHeading" values={{ number: formValues.streams.length }} />
            </Text>
            <InfoTooltip placement="top">
              <FormattedMessage id="connectorBuilder.streamTooltip" />
            </InfoTooltip>
          </FlexContainer>

          <AddStreamButton
            onAddStream={(addedStreamNum) => handleViewSelect(addedStreamNum)}
            disabled={permission === "readOnly"}
            data-testid="add-stream"
          />
        </FlexContainer>

        <div className={styles.streamList}>
          {formValues.streams.map(({ name, id }, num) => (
            <ViewSelectButton
              key={num}
              data-testid={`navbutton-${String(num)}`}
              selected={view === num}
              showErrorIndicator={hasErrors([num])}
              onClick={() => {
                handleViewSelect(num);
                analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_SELECT, {
                  actionDescription: "Stream view selected",
                  stream_id: id,
                  stream_name: name,
                });
              }}
            >
              {name && name.trim() ? (
                <Text className={styles.streamViewText}>{name}</Text>
              ) : (
                <Text className={styles.emptyStreamViewText}>
                  <FormattedMessage id="connectorBuilder.emptyName" />
                </Text>
              )}
            </ViewSelectButton>
          ))}
        </div>
      </FlexContainer>
    </Sidebar>
  );
};
