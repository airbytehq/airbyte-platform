import { faSliders, faUser } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classnames from "classnames";
import { useFormikContext } from "formik";
import React from "react";
import { FormattedMessage } from "react-intl";

import Indicator from "components/Indicator";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { Action, Namespace } from "core/analytics";
import { useAnalyticsService } from "hooks/services/Analytics";
import { BuilderView, useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { AddStreamButton } from "./AddStreamButton";
import styles from "./BuilderSidebar.module.scss";
import { SavingIndicator } from "./SavingIndicator";
import { UiYamlToggleButton } from "./UiYamlToggleButton";
import { CDK_VERSION } from "../cdk";
import { ConnectorImage } from "../ConnectorImage";
import { DownloadYamlButton } from "../DownloadYamlButton";
import { PublishButton } from "../PublishButton";
import { BuilderFormValues, getInferredInputs } from "../types";
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
  const { values } = useFormikContext<BuilderFormValues>();
  const handleViewSelect = (selectedView: BuilderView) => {
    setSelectedView(selectedView);
  };

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
          showErrorIndicator={hasErrors(true, ["global"])}
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
                number: values.inputs.length + getInferredInputs(values.global, values.inferredInputOverrides).length,
              }}
            />
          </Text>
        </ViewSelectButton>
      </FlexContainer>

      <FlexContainer direction="column" alignItems="stretch" gap="none" className={styles.streamList}>
        <div className={styles.streamsHeader}>
          <Text className={styles.streamsHeading} size="xs" bold>
            <FormattedMessage id="connectorBuilder.streamsHeading" values={{ number: values.streams.length }} />
          </Text>

          <AddStreamButton
            onAddStream={(addedStreamNum) => handleViewSelect(addedStreamNum)}
            data-testid="add-stream"
          />
        </div>

        {values.streams.map(({ name, id }, num) => (
          <ViewSelectButton
            key={num}
            data-testid={`navbutton-${String(num)}`}
            selected={selectedView === num}
            showErrorIndicator={hasErrors(true, [num])}
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
      </FlexContainer>
      <FlexContainer direction="column" alignItems="stretch" gap="md">
        <DownloadYamlButton yamlIsValid yaml={yamlManifest} />
        <PublishButton />
      </FlexContainer>
      <Text size="sm" color="grey" centered>
        <FormattedMessage
          id="connectorBuilder.cdkVersion"
          values={{
            version: CDK_VERSION,
          }}
        />
      </Text>
    </FlexContainer>
  );
});
