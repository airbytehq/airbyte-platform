import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { MapperConfiguration, StreamMapperType } from "core/api/types/AirbyteClient";

import { useMappingContext } from "./MappingContext";
import styles from "./MappingRow.module.scss";
import { OperationType } from "./RowFilteringMapperForm";
import { StreamMapperWithId } from "./types";

interface MappingTypeListBoxProps {
  selectedValue: StreamMapperType;
  streamDescriptorKey: string;
  mappingId: string;
}

export const MappingTypeListBox: React.FC<MappingTypeListBoxProps> = ({
  selectedValue,
  streamDescriptorKey,
  mappingId,
}) => {
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

  const supportedMappingsOptions = Object.values(StreamMapperType).map((type) => ({
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

  // todo: support partial/empty config!
  return (
    <ListBox
      options={supportedMappingsOptions}
      selectedValue={selectedValue}
      controlButton={ControlButton}
      buttonClassName={styles.controlButton}
      onSelect={(value) => {
        if (value !== selectedValue) {
          let validConfiguration: StreamMapperWithId<MapperConfiguration>;
          switch (value) {
            case StreamMapperType["row-filtering"]:
              validConfiguration = {
                type: value,
                id: mappingId,
                validationCallback: () => Promise.reject(false),
                mapperConfiguration: {
                  conditions: { type: OperationType.equal, fieldName: "", comparisonValue: "" },
                },
              };
              break;
            case StreamMapperType.hashing:
              validConfiguration = {
                type: value,
                id: mappingId,
                validationCallback: () => Promise.reject(false),
                mapperConfiguration: {
                  targetField: "",
                  method: "MD5",
                  fieldNameSuffix: "_hashed",
                },
              };
              break;
            case StreamMapperType["field-renaming"]:
              validConfiguration = {
                type: value,
                id: mappingId,
                validationCallback: () => Promise.reject(false),
                mapperConfiguration: { originalFieldName: "", newFieldName: "" },
              };
              break;
            case StreamMapperType.encryption:
              validConfiguration = {
                type: value,
                id: mappingId,
                validationCallback: () => Promise.reject(false),
                mapperConfiguration: {
                  algorithm: "AES",
                  targetField: "",
                  key: "",
                  fieldNameSuffix: "_encrypted",
                  mode: "CBC",
                  padding: "PKCS5Padding",
                },
              };
              break;
            default:
              throw new Error(`Unsupported StreamMapperType: ${value}`);
          }
          updateLocalMapping(streamDescriptorKey, validConfiguration.id, validConfiguration);
        }
      }}
    />
  );
};
