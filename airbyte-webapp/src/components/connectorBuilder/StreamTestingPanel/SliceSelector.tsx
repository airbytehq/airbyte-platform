import { faAngleDown } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { useCallback, useMemo } from "react";
import { useIntl } from "react-intl";

import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { StreamReadSlicesItem } from "core/request/ConnectorBuilderClient";

import styles from "./SliceSelector.module.scss";

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

export const SliceSelector: React.FC<SliceSelectorProps> = ({ className, slices, selectedSliceIndex, onSelect }) => {
  const { formatMessage } = useIntl();

  const getSliceLabel = useCallback(
    (_slice: StreamReadSlicesItem, sliceIndex: number) => {
      // TODO: Use slice_descriptor for better messages
      return `${formatMessage({ id: "connectorBuilder.sliceLabel" })} ${sliceIndex}`;
    },
    [formatMessage]
  );

  const options = useMemo(
    () =>
      slices.map((slice, index) => {
        return { label: getSliceLabel(slice, index), value: index };
      }),
    [slices, getSliceLabel]
  );

  return (
    <ListBox
      data-testid="tag-select-slice"
      className={className}
      options={options}
      selectedValue={selectedSliceIndex}
      onSelect={(selected) => onSelect(selected)}
      buttonClassName={styles.button}
      controlButton={ControlButton}
    />
  );
};
