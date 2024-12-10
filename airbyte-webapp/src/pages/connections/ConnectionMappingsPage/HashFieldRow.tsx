import { useMemo } from "react";
import { Controller, FormProvider, useForm, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { v4 as uuidv4 } from "uuid";
import * as yup from "yup";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { MapperConfiguration, StreamMapperType } from "core/api/types/AirbyteClient";

import { autoSubmitResolver } from "./autoSubmitResolver";
import { useMappingContext } from "./MappingContext";
import styles from "./MappingRow.module.scss";
import { MappingTypeListBox } from "./MappingTypeListBox";
import { SelectTargetField } from "./SelectTargetField";
import { useGetFieldsInStream } from "./useGetFieldsInStream";

export enum HashingMethods {
  MD2 = "MD2",
  MD5 = "MD5",
  SHA1 = "SHA-1",
  SHA224 = "SHA-224",
  SHA256 = "SHA-256",
  SHA384 = "SHA-384",
  SHA512 = "SHA-512",
}

export interface HashingMapperFormValues {
  type: StreamMapperType;
  mapperConfiguration: {
    targetField: string;
    method: HashingMethods;
    fieldNameSuffix: string;
  };
}

export interface HashingMapperRowProps {
  type: StreamMapperType;
  mapperConfiguration: MapperConfiguration;
}

const hashingMapperConfigSchema = yup.object().shape({
  targetField: yup.string().required("Target field is required"),
  method: yup.mixed<HashingMethods>().oneOf(Object.values(HashingMethods)).required("Hashing method is required"),
  fieldNameSuffix: yup.string().required("Field name suffix is required"),
});

export const hashingMapperSchema = yup.object().shape({
  type: yup.mixed<StreamMapperType>().oneOf(["hashing"]).required(),
  mapperConfiguration: hashingMapperConfigSchema,
});

export const HashFieldRow: React.FC<{
  mappingId: string;
  streamName: string;
}> = ({ mappingId, streamName }) => {
  const { updateLocalMapping, streamsWithMappings, validateMappings } = useMappingContext();
  const mapping = streamsWithMappings[streamName].find((m) => m.mapperConfiguration.id === mappingId);
  const fieldsInStream = useGetFieldsInStream(streamName);

  const defaultValues = useMemo(() => {
    return {
      type: StreamMapperType.hashing,
      mapperConfiguration: {
        id: mapping?.mapperConfiguration.id ?? uuidv4(),
        targetField: mapping?.mapperConfiguration.targetField ?? "",
        method: mapping?.mapperConfiguration.method ?? HashingMethods.MD5,
        fieldNameSuffix: mapping?.mapperConfiguration.fieldNameSuffix ?? "_hashed",
      },
    };
  }, [mapping]);

  const methods = useForm<HashingMapperFormValues>({
    defaultValues,
    resolver: autoSubmitResolver<HashingMapperFormValues>(hashingMapperSchema, (data) => {
      updateLocalMapping(streamName, data);
      validateMappings();
    }),
    mode: "onBlur",
  });

  if (!mapping) {
    return null;
  }

  return (
    <FormProvider {...methods}>
      <form>
        <FlexContainer direction="row" alignItems="center" justifyContent="space-between" className={styles.rowContent}>
          <MappingTypeListBox
            selectedValue={StreamMapperType.hashing}
            mappingId={mapping.mapperConfiguration.id}
            streamName={streamName}
          />
          <SelectTargetField<HashingMapperFormValues>
            name="mapperConfiguration.targetField"
            targetFieldOptions={fieldsInStream}
          />
          <Text>
            <FormattedMessage id="connections.mappings.using" />
          </Text>
          <SelectHashingMethod />
        </FlexContainer>
      </form>
    </FormProvider>
  );
};

const SelectHashingMethodControlButton: React.FC<ListBoxControlButtonProps<HashingMethods>> = ({ selectedOption }) => {
  if (!selectedOption) {
    return (
      <Text color="grey">
        <FormattedMessage id="connections.mappings.hashing.method" />
      </Text>
    );
  }

  return (
    <FlexContainer alignItems="center" gap="none">
      <Text>{selectedOption.label}</Text>
      <Icon type="caretDown" color="disabled" />
    </FlexContainer>
  );
};

const supportedHashTypes = [
  { label: "MD5", value: HashingMethods.MD5 },
  { label: "SHA-256", value: HashingMethods.SHA256 },
  { label: "SHA-512", value: HashingMethods.SHA512 },
];

const SelectHashingMethod = () => {
  const { control } = useFormContext<HashingMapperFormValues>();

  return (
    <Controller
      name="mapperConfiguration.method"
      control={control}
      defaultValue={HashingMethods.MD5}
      render={({ field }) => (
        <ListBox
          buttonClassName={styles.controlButton}
          controlButton={SelectHashingMethodControlButton}
          onSelect={(value) => {
            field.onChange(value);
            // We're using onBlur mode, so we need to manually trigger the validation
            field.onBlur();
          }}
          selectedValue={field.value}
          options={supportedHashTypes}
        />
      )}
    />
  );
};
