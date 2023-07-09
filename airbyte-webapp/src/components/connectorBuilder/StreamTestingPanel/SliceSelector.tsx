import { faAngleDown } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { useMemo } from "react";
import React from "react";
import { useIntl } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { StreamReadSlicesItem } from "core/api/types/ConnectorBuilderClient";

import styles from "./SliceSelector.module.scss";
import { formatJson } from "../utils";

interface SliceSelectorProps {
  className?: string;
  slices: StreamReadSlicesItem[];
  selectedSliceIndex: number;
  onSelect: (sliceIndex: number) => void;
}

const ControlButton: React.FC<ListBoxControlButtonProps<number>> = ({ selectedOption }) => {
  return (
    <>
      {selectedOption && <Text size="md">{selectedOption.label}</Text>}
      <FontAwesomeIcon className={styles.arrow} icon={faAngleDown} />
    </>
  );
};

const getSliceLabel = (slice: StreamReadSlicesItem) => {
  if (!slice.slice_descriptor) {
    return null;
  }

  const { slice_descriptor } = slice;
  return (
    <div className={styles.sliceDescriptor}>
      {Object.entries(slice_descriptor).map(([key, value]) => {
        return (
          <React.Fragment key={key}>
            <Text size="md" className={styles.overflowText}>{`${key}: `}</Text>
            <Text bold className={styles.overflowText}>
              {typeof value === "string" ? value : formatJson(value)}
            </Text>
          </React.Fragment>
        );
      })}
    </div>
  );
};

export const SliceSelector: React.FC<SliceSelectorProps> = ({ className, slices, selectedSliceIndex, onSelect }) => {
  const { formatMessage } = useIntl();

  const options = useMemo(
    () =>
      slices.map((_slice, index) => {
        return {
          label: `${formatMessage({ id: "connectorBuilder.sliceLabel" })}\u00A0${index + 1}`,
          value: index,
        };
      }),
    [formatMessage, slices]
  );

  return (
    <FlexContainer>
      <ListBox
        data-testid="tag-select-slice"
        className={className}
        options={options}
        selectedValue={selectedSliceIndex}
        onSelect={(selected) => onSelect(selected)}
        buttonClassName={styles.button}
        controlButton={ControlButton}
      />
      {selectedSliceIndex >= 0 && <FlexItem grow>{getSliceLabel(slices[selectedSliceIndex])}</FlexItem>}
    </FlexContainer>
  );
};
