import { faSliders, faUser } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classnames from "classnames";
import { useFormikContext } from "formik";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import Indicator from "components/Indicator";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { Action, Namespace } from "core/analytics";
import { useAnalyticsService } from "hooks/services/Analytics";
import { BuilderView, useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { AddStreamButton } from "./AddStreamButton";
import styles from "./BuilderSidebar.module.scss";
import { SavingIndicator } from "./SavingIndicator";
import { UiYamlToggleButton } from "./UiYamlToggleButton";
import { DownloadYamlButton } from "../DownloadYamlButton";
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
  const navigate = useNavigate();
  const analyticsService = useAnalyticsService();
  const { formatMessage } = useIntl();
  const { hasErrors } = useBuilderErrors();
  const { yamlManifest, selectedView, setSelectedView } = useConnectorBuilderFormState();
  const { values } = useFormikContext<BuilderFormValues>();
  const goToListing = () => {
    navigate("..");
  };
  const handleViewSelect = (selectedView: BuilderView) => {
    setSelectedView(selectedView);
  };

  return (
    <FlexContainer direction="column" alignItems="stretch" gap="xl" className={classnames(className, styles.container)}>
      <UiYamlToggleButton yamlSelected={false} onClick={toggleYamlEditor} />

      <FlexContainer direction="column" alignItems="center">
        {/* TODO: replace with uploaded img when that functionality is added */}
        <img
          className={styles.connectorImg}
          src="/logo.png"
          alt={formatMessage({ id: "connectorBuilder.connectorImgAlt" })}
        />

        <div className={styles.connectorName}>
          <Heading as="h2" size="sm">
            {values.global?.connectorName}
          </Heading>
        </div>

        <FlexItem>
          <SavingIndicator />
        </FlexItem>
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
          <FormattedMessage id="connectorBuilder.globalConfiguration" />
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
          <FormattedMessage
            id="connectorBuilder.userInputs"
            values={{
              number: values.inputs.length + getInferredInputs(values.global, values.inferredInputOverrides).length,
            }}
          />
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
      <FlexContainer direction="column" alignItems="stretch" gap="sm">
        <DownloadYamlButton className={styles.downloadButton} yamlIsValid yaml={yamlManifest} />
        <Button className={styles.resetButton} full variant="clear" onClick={goToListing}>
          <FormattedMessage id="connectorBuilder.backToListing" />
        </Button>
      </FlexContainer>
    </FlexContainer>
  );
});
