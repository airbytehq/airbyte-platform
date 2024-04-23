import classNames from "classnames";
import capitalize from "lodash/capitalize";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./StreamSelector.module.scss";
import { useBuilderWatch } from "../types";

interface StreamSelectorProps {
  className?: string;
}

const ControlButton: React.FC<ListBoxControlButtonProps<string>> = ({ selectedOption }) => {
  return (
    <>
      {selectedOption && (
        <Heading className={styles.label} as="h1" size="sm">
          {selectedOption.label}
        </Heading>
      )}
      <Icon type="caretDown" color="primary" />
    </>
  );
};

export const StreamSelector: React.FC<StreamSelectorProps> = ({ className }) => {
  const analyticsService = useAnalyticsService();
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();
  const view = useBuilderWatch("view");
  const testStreamIndex = useBuilderWatch("testStreamIndex");

  const { streamNames } = useConnectorBuilderFormState();

  if (streamNames.length === 0) {
    return (
      <Box py="md">
        <Heading className={styles.label} as="h1" size="sm">
          -
        </Heading>
      </Box>
    );
  }

  const options = streamNames.map((streamName) => {
    const label =
      streamName && streamName.trim() ? capitalize(streamName) : formatMessage({ id: "connectorBuilder.emptyName" });
    return { label, value: streamName ?? "" };
  });

  const handleStreamSelect = (selectedStreamName: string) => {
    const selectedStreamIndex = streamNames.findIndex((streamName) => selectedStreamName === streamName);
    if (selectedStreamIndex >= 0) {
      setValue("testStreamIndex", selectedStreamIndex);

      if (view !== "global" && view !== "inputs") {
        setValue("view", selectedStreamIndex);
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_SELECT, {
          actionDescription: "Stream view selected in testing panel",
          stream_name: selectedStreamName,
        });
      }
    }
  };

  return (
    <ListBox
      className={classNames(className, styles.container)}
      options={options}
      selectedValue={streamNames[testStreamIndex] ?? formatMessage({ id: "connectorBuilder.noStreamSelected" })}
      onSelect={handleStreamSelect}
      buttonClassName={styles.button}
      controlButton={ControlButton}
    />
  );
};
