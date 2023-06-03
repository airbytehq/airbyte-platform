import { faSliders, faUser } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classnames from "classnames";
import React from "react";
import { useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import Indicator from "components/Indicator";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { InfoTooltip, Tooltip } from "components/ui/Tooltip";

import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { BuilderView, useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";
import { links } from "utils/links";

import { AddStreamButton } from "./AddStreamButton";
import styles from "./BuilderSidebar.module.scss";
import { SavingIndicator } from "./SavingIndicator";
import { ReactComponent as SlackIcon } from "./slack-icon.svg";
import { UiYamlToggleButton } from "./UiYamlToggleButton";
import { ConnectorImage } from "../ConnectorImage";
import { DownloadYamlButton } from "../DownloadYamlButton";
import { PublishButton } from "../PublishButton";
import { BuilderFormValues } from "../types";
import { useBuilderErrors } from "../useBuilderErrors";
import { useInferredInputs } from "../useInferredInputs";

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
  toggleYamlEditor: () => void;
}

export const BuilderSidebar: React.FC<BuilderSidebarProps> = React.memo(({ className, toggleYamlEditor }) => {
  const analyticsService = useAnalyticsService();
  const { hasErrors } = useBuilderErrors();
  const { yamlManifest, selectedView, setSelectedView, builderFormValues } = useConnectorBuilderFormState();
  const values = useWatch<BuilderFormValues>();
  const handleViewSelect = (selectedView: BuilderView) => {
    setSelectedView(selectedView);
  };

  const inferredInputsLength = useInferredInputs().length;

  return (
    <FlexContainer direction="column" alignItems="stretch" gap="xl" className={classnames(className, styles.container)}>
      <UiYamlToggleButton yamlSelected={false} onClick={toggleYamlEditor} />

      <FlexContainer direction="column" alignItems="center">
        <ConnectorImage />

        <div className={styles.connectorName}>
          <Heading as="h2" size="sm">
            {values.global?.connectorName}
          </Heading>
        </div>

        {builderFormValues.streams.length > 0 && <SavingIndicator />}
      </FlexContainer>

      <FlexContainer direction="column" alignItems="stretch" gap="none">
        <ViewSelectButton
          data-testid="navbutton-global"
          selected={selectedView === "global"}
          showErrorIndicator={hasErrors(["global"])}
          onClick={() => {
            handleViewSelect("global");
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.GLOBAL_CONFIGURATION_SELECT, {
              actionDescription: "Global Configuration view selected",
            });
          }}
        >
          <FontAwesomeIcon icon={faSliders} />
          <Text className={styles.streamViewText}>
            <FormattedMessage id="connectorBuilder.globalConfiguration" />
          </Text>
        </ViewSelectButton>

        <ViewSelectButton
          data-testid="navbutton-inputs"
          showErrorIndicator={false}
          selected={selectedView === "inputs"}
          onClick={() => {
            handleViewSelect("inputs");
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.USER_INPUTS_SELECT, {
              actionDescription: "User Inputs view selected",
            });
          }}
        >
          <FontAwesomeIcon icon={faUser} />
          <Text className={styles.streamViewText}>
            <FormattedMessage
              id="connectorBuilder.userInputs"
              values={{
                number: (values.inputs?.length ?? 0) + inferredInputsLength,
              }}
            />
          </Text>
        </ViewSelectButton>
      </FlexContainer>

      <FlexContainer direction="column" alignItems="stretch" gap="sm" className={styles.streamListContainer}>
        <FlexContainer className={styles.streamsHeader} alignItems="center" justifyContent="space-between">
          <FlexContainer alignItems="center" gap="none">
            <Text className={styles.streamsHeading} size="xs" bold>
              <FormattedMessage id="connectorBuilder.streamsHeading" values={{ number: values.streams?.length }} />
            </Text>
            <InfoTooltip placement="top">
              <FormattedMessage id="connectorBuilder.streamTooltip" />
            </InfoTooltip>
          </FlexContainer>

          <AddStreamButton
            onAddStream={(addedStreamNum) => handleViewSelect(addedStreamNum)}
            data-testid="add-stream"
          />
        </FlexContainer>

        <div className={styles.streamList}>
          {values.streams?.map(({ name, id }, num) => (
            <ViewSelectButton
              key={num}
              data-testid={`navbutton-${String(num)}`}
              selected={selectedView === num}
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
      <FlexContainer direction="column" alignItems="stretch" gap="md">
        <DownloadYamlButton yamlIsValid yaml={yamlManifest} />
        <PublishButton />
      </FlexContainer>
      <FlexContainer direction="column" gap="lg">
        <Tooltip
          placement="top"
          control={
            <Text size="sm" className={styles.slackLink}>
              <a href="https://airbytehq.slack.com/archives/C027KKE4BCZ" target="_blank" rel="noreferrer">
                <FlexContainer gap="sm" justifyContent="center" alignItems="flex-start">
                  <SlackIcon className={styles.slackIcon} />
                  <FormattedMessage id="connectorBuilder.slackChannel" />
                </FlexContainer>
              </a>
            </Text>
          }
        >
          <FormattedMessage id="connectorBuilder.slackChannelTooltip" />
        </Tooltip>
        <Text size="sm" className={styles.slackLink}>
          <a href={links.connectorBuilderTutorial} target="_blank" rel="noreferrer">
            <FlexContainer gap="sm" justifyContent="center" alignItems="flex-start">
              <Icon type="docs" />
              <FormattedMessage id="connectorBuilder.createPage.tutorialPrompt" />
            </FlexContainer>
          </a>
        </Text>
      </FlexContainer>
    </FlexContainer>
  );
});
BuilderSidebar.displayName = "BuilderSidebar";
