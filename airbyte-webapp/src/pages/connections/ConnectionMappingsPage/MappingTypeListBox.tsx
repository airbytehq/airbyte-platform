import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { MapperConfiguration, StreamMapperType } from "core/api/types/AirbyteClient";

import { useMappingContext } from "./MappingContext";
import styles from "./MappingRow.module.scss";
import { StreamMapperWithId } from "./types";

enum OperationType {
  equal = "EQUAL",
  not = "NOT",
}

export const supportedMappings = ["hashing", "field-renaming", "row-filtering", "encryption"] as const;
export type SupportedMapping = (typeof supportedMappings)[number];

interface MappingTypeListBoxProps {
  selectedValue: SupportedMapping;
  streamDescriptorKey: string;
  mappingId: string;
  disabled: boolean;
}

export const MappingTypeListBox: React.FC<MappingTypeListBoxProps> = ({
  selectedValue,
  streamDescriptorKey,
  mappingId,
  disabled,
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
  } as const;
  const supportedMappingsOptions = supportedMappings.map((type) => ({
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

  const ControlButton: React.FC<ListBoxControlButtonProps<SupportedMapping>> = ({ selectedOption, isDisabled }) => {
    if (!selectedOption) {
      return (
        <Text color="grey" as="span">
          <FormattedMessage id="connections.mappings.mappingType" />
        </Text>
      );
    }

    return (
      <FlexContainer alignItems="center" gap="none" as="span">
        <Text as="span" color={isDisabled ? "grey300" : "darkBlue"}>
          <FormattedMessage id={mappingTypeLabels[selectedOption.value].title} />
        </Text>
      </FlexContainer>
    );
  };

  return (
    <ListBox
      options={supportedMappingsOptions}
      selectedValue={selectedValue}
      controlButtonContent={ControlButton}
      buttonClassName={styles.controlButton}
      isDisabled={disabled}
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
                  algorithm: "RSA",
                  publicKey: "",
                  fieldNameSuffix: "_encrypted",
                  targetField: "",
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
