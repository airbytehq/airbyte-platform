import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { Heading } from "components/ui/Heading";
import { ListBox, ListBoxControlButtonProps, Option } from "components/ui/ListBox";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConnectorBuilderFormState } from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import styles from "./StreamSelector.module.scss";
import { StreamId } from "../types";
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

const ControlButton: React.FC<ListBoxControlButtonProps<SelectorOption>> = ({ selectedOption }) => (
  <Heading className={styles.label} as="h1" size="sm">
    {selectedOption?.label ?? ""}
  </Heading>
);

export const StreamSelector: React.FC<StreamSelectorProps> = () => {
  const analyticsService = useAnalyticsService();
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();
  const view = useBuilderWatch("view");
  const testStreamId = useBuilderWatch("testStreamId");
  const generatedStreams = useBuilderWatch("formValues.generatedStreams");
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

    let selectedStreamId: StreamId | undefined;

    if (type === "stream") {
      selectedStreamId = {
        type: "stream" as const,
        index: streamNames.findIndex((streamName) => selectedStreamName === streamName),
      };
    } else if (type === "dynamic_stream") {
      const selectedStreamIndex = dynamicStreamNames.findIndex((streamName) => selectedStreamName === streamName);
      selectedStreamId = { type: "dynamic_stream" as const, index: selectedStreamIndex };
    } else if (type === "generated_stream") {
      const selectedGeneratedStreams = generatedStreams[selectedStream.dynamicStreamName];
      const selectedStreamIndex = selectedGeneratedStreams.findIndex((stream) => selectedStreamName === stream.name);
      selectedStreamId = {
        type: "generated_stream" as const,
        index: selectedStreamIndex,
        dynamicStreamName: selectedStream.dynamicStreamName,
      };
    }

    if (selectedStreamId != null) {
      setValue("testStreamId", selectedStreamId);

      if (view.type !== "global" && view.type !== "inputs") {
        setValue("view", selectedStreamId);
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_SELECT, {
          actionDescription: "Stream view selected in testing panel",
          stream_name: selectedStreamName,
        });
      }
    }
  };

  const selectedValueType: SelectorOption["type"] = testStreamId.type;

  const selectedValue = options.find((option) => {
    if (option.value.type === "generated_stream") {
      return (
        option.value.type === selectedValueType &&
        option.value.idx === testStreamId.index &&
        option.value.dynamicStreamName === ("dynamicStreamName" in testStreamId ? testStreamId.dynamicStreamName : "")
      );
    }
    return option.value.type === selectedValueType && option.value.idx === testStreamId.index;
  })?.value;

  return (
    <ListBox
      options={options}
      selectedValue={selectedValue}
      onSelect={handleStreamSelect}
      buttonClassName={styles.button}
      controlButtonContent={ControlButton}
    />
  );
};
