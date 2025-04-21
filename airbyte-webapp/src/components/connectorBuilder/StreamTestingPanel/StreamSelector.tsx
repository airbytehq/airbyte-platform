import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ListBox, ListBoxControlButtonProps, Option } from "components/ui/ListBox";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./StreamSelector.module.scss";
import { BuilderState, StreamId } from "../types";
import { useBuilderWatch } from "../useBuilderWatch";

interface StreamSelectorProps {
  className?: string;
}

interface GeneratedStreamOption {
  type: "generated_stream";
  name: string;
  idx: number;
  dynamicStreamName: string;
}

interface BaseSelectorOption {
  type: "stream" | "dynamic_stream";
  name: string;
  idx: number;
}

type SelectorOption = BaseSelectorOption | GeneratedStreamOption;

const ControlButton: React.FC<ListBoxControlButtonProps<SelectorOption>> = ({ selectedOption }) => {
  return (
    <>
      {selectedOption && (
        <Heading className={styles.label} as="h1" size="sm">
          {selectedOption.label}
        </Heading>
      )}
      <Icon className={styles.caret} type="caretDown" color="primary" />
    </>
  );
};

export const StreamSelector: React.FC<StreamSelectorProps> = ({ className }) => {
  const analyticsService = useAnalyticsService();
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();
  const view = useBuilderWatch("view");
  const testStreamId = useBuilderWatch("testStreamId");
  const generatedStreams = useBuilderWatch("generatedStreams");
  const { streamNames, dynamicStreamNames } = useConnectorBuilderFormState();

  if (streamNames.length === 0 && dynamicStreamNames.length === 0) {
    return (
      <Box py="md">
        <Heading className={styles.label} as="h1" size="sm">
          -
        </Heading>
      </Box>
    );
  }

  const options: Array<Option<SelectorOption>> = [];

  options.push(
    ...dynamicStreamNames.map((streamName, idx) => {
      const label = streamName.trim() ? streamName : formatMessage({ id: "connectorBuilder.emptyName" });
      return { label, value: { type: "dynamic_stream" as const, name: streamName ?? "", idx } };
    })
  );

  Object.entries(generatedStreams).forEach(([dynamicStreamName, streams]) => {
    options.push(
      ...streams.map((stream, idx) => {
        const label = stream.name?.trim()
          ? `${dynamicStreamName}: ${stream.name}`
          : formatMessage({ id: "connectorBuilder.emptyName" });
        return { label, value: { type: "generated_stream" as const, name: stream.name ?? "", idx, dynamicStreamName } };
      })
    );
  });

  options.push(
    ...streamNames.map((streamName, idx) => {
      const label = streamName.trim() ? streamName : formatMessage({ id: "connectorBuilder.emptyName" });
      return { label, value: { type: "stream" as const, name: streamName ?? "", idx } };
    })
  );

  const handleStreamSelect = (selectedStream: SelectorOption) => {
    const { type, name: selectedStreamName } = selectedStream;

    let selectedView: BuilderState["view"] | undefined;
    let selectedStreamId: StreamId | undefined;

    if (type === "stream") {
      selectedView = streamNames.findIndex((streamName) => selectedStreamName === streamName);
      selectedStreamId = { type: "stream" as const, index: selectedView };
    } else if (type === "dynamic_stream") {
      const selectedStreamIndex = dynamicStreamNames.findIndex((streamName) => selectedStreamName === streamName);
      selectedView = `dynamic_stream_${selectedStreamIndex}`;
      selectedStreamId = { type: "dynamic_stream" as const, index: selectedStreamIndex };
    } else if (type === "generated_stream") {
      const selectedGeneratedStreams = generatedStreams[selectedStream.dynamicStreamName];
      const selectedStreamIndex = selectedGeneratedStreams.findIndex((stream) => selectedStreamName === stream.name);
      selectedView = `generated_stream_${selectedStreamIndex}`;
      selectedStreamId = {
        type: "generated_stream" as const,
        index: selectedStreamIndex,
        dynamicStreamName: selectedStream.dynamicStreamName,
      };
    }

    if (selectedView != null) {
      setValue("testStreamId", selectedStreamId);

      if (view !== "global" && view !== "inputs") {
        setValue("view", selectedView);
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_SELECT, {
          actionDescription: "Stream view selected in testing panel",
          stream_name: selectedStreamName,
        });
      }
    }
  };

  const selectedValueType: SelectorOption["type"] = testStreamId.type;

  const selectedValue = options.find(
    (option) => option.value.type === selectedValueType && option.value.idx === testStreamId.index
  )?.value;

  return (
    <ListBox
      className={className}
      options={options}
      selectedValue={selectedValue}
      onSelect={handleStreamSelect}
      buttonClassName={styles.button}
      controlButton={ControlButton}
    />
  );
};
