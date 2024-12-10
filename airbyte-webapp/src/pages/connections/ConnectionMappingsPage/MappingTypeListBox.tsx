import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { StreamMapperType } from "core/api/types/AirbyteClient";

import { useMappingContext } from "./MappingContext";
import { SupportedMappingTypes } from "./MappingRow";
import styles from "./MappingRow.module.scss";
import { OperationType } from "./RowFilterRow";

interface MappingTypeListBoxProps {
  selectedValue: StreamMapperType;
  streamName: string;
  mappingId: string;
}

export const MappingTypeListBox: React.FC<MappingTypeListBoxProps> = ({ selectedValue, streamName, mappingId }) => {
  const { updateLocalMapping } = useMappingContext();

  const mappingTypeLabels = {
    hashing: { title: "connections.mappings.type.hash", description: "connections.mappings.type.hash.description" },
    "field-renaming": {
      title: "connections.mappings.type.fieldRenaming",
      description: "connections.mappings.type.fieldRenaming.description",
    },
    "row-filtering": {
      title: "connections.mappings.type.rowFiltering",
      description: "connections.mappings.type.rowFiltering.description",
    },
    encryption: {
      title: "connections.mappings.type.encryption",
      description: "connections.mappings.type.encryption.description",
    },
  };

  const supportedMappingsOptions = Object.values(SupportedMappingTypes).map((type) => ({
    label: (
      <FlexContainer direction="column" gap="xs" as="span">
        <Text as="span">
          <FormattedMessage id={mappingTypeLabels[type].title} />
        </Text>
        <Text color="grey500" as="span">
          <FormattedMessage id={mappingTypeLabels[type].description} />
        </Text>
      </FlexContainer>
    ),
    value: type,
  }));

  const ControlButton: React.FC<ListBoxControlButtonProps<StreamMapperType>> = ({ selectedOption }) => {
    if (!selectedOption) {
      return (
        <Text color="grey" as="span">
          <FormattedMessage id="connections.mappings.mappingType" />
        </Text>
      );
    }

    return (
      <FlexContainer alignItems="center" gap="none" as="span">
        <Text as="span">
          <FormattedMessage id={mappingTypeLabels[selectedOption.value].title} />
        </Text>
        <Icon type="caretDown" color="disabled" />
      </FlexContainer>
    );
  };

  return (
    <ListBox
      options={supportedMappingsOptions}
      selectedValue={selectedValue}
      controlButton={ControlButton}
      buttonClassName={styles.controlButton}
      onSelect={(value) => {
        if (value !== selectedValue) {
          if (value === StreamMapperType["row-filtering"]) {
            updateLocalMapping(streamName, {
              type: value,
              mapperConfiguration: { id: mappingId, conditions: { type: OperationType.equal } },
            });
          } else {
            updateLocalMapping(streamName, { type: value, mapperConfiguration: { id: mappingId } });
          }
        }
      }}
    />
  );
};
