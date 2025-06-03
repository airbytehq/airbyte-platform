import React, { useMemo } from "react";
import { useIntl } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { StreamReadSlicesItem } from "core/api/types/ConnectorBuilderClient";

import { InnerListBox } from "./InnerListBox";
import styles from "./SliceSelector.module.scss";
import { formatJson } from "../utils";

interface SliceSelectorProps {
  className?: string;
  slices: StreamReadSlicesItem[];
  selectedSliceIndex: number;
  onSelect: (sliceIndex: number) => void;
}

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
      <InnerListBox
        data-testid="tag-select-slice"
        className={className}
        options={options}
        selectedValue={selectedSliceIndex}
        onSelect={(selected) => onSelect(selected)}
      />
      {selectedSliceIndex >= 0 && <FlexItem grow>{getSliceLabel(slices[selectedSliceIndex])}</FlexItem>}
    </FlexContainer>
  );
};
