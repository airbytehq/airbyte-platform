import { useMemo } from "react";
import { Controller, FormProvider, useForm, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import * as yup from "yup";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ListBox, ListBoxControlButtonProps } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import {
  HashingMapperConfiguration,
  HashingMapperConfigurationMethod,
  StreamMapperType,
} from "core/api/types/AirbyteClient";

import { autoSubmitResolver } from "./autoSubmitResolver";
import { useMappingContext } from "./MappingContext";
import styles from "./MappingRow.module.scss";
import { MappingTypeListBox } from "./MappingTypeListBox";
import { SelectTargetField } from "./SelectTargetField";
import { StreamMapperWithId } from "./types";
import { useGetFieldsInStream } from "./useGetFieldsInStream";
export interface HashingMapperFormValues {
  type: StreamMapperType;
  id: string;
  mapperConfiguration: HashingMapperConfiguration;
}

const hashingMapperConfigSchema = yup.object().shape({
  targetField: yup.string().required("Target field is required"),
  method: yup
    .mixed<HashingMapperConfigurationMethod>()
    .oneOf(Object.values(HashingMapperConfigurationMethod))
    .required("Hashing method is required"),
  fieldNameSuffix: yup.string().required("Field name suffix is required"),
});

export const hashingMapperSchema = yup.object().shape({
  type: yup.mixed<StreamMapperType>().oneOf(["hashing"]).required(),
  id: yup.string().required(),
  mapperConfiguration: hashingMapperConfigSchema,
});

export const HashFieldRow: React.FC<{
  mapping: StreamMapperWithId<HashingMapperConfiguration>;
  streamName: string;
}> = ({ mapping, streamName }) => {
  const { updateLocalMapping, validateMappings } = useMappingContext();
  const fieldsInStream = useGetFieldsInStream(streamName);

  const defaultValues = useMemo(() => {
    return {
      type: StreamMapperType.hashing,
      mapperConfiguration: {
        id: mapping.id,
        targetField: mapping.mapperConfiguration.targetField ?? "",
        method: mapping.mapperConfiguration.method ?? HashingMapperConfigurationMethod.MD5,
        fieldNameSuffix: mapping.mapperConfiguration.fieldNameSuffix ?? "_hashed",
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
          <MappingTypeListBox selectedValue={StreamMapperType.hashing} mappingId={mapping.id} streamName={streamName} />
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

const SelectHashingMethodControlButton: React.FC<ListBoxControlButtonProps<HashingMapperConfigurationMethod>> = ({
  selectedOption,
}) => {
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
  { label: "MD5", value: HashingMapperConfigurationMethod.MD5 },
  { label: "SHA-256", value: HashingMapperConfigurationMethod["SHA-256"] },
  { label: "SHA-512", value: HashingMapperConfigurationMethod["SHA-512"] },
];

const SelectHashingMethod = () => {
  const { control } = useFormContext<HashingMapperFormValues>();

  return (
    <Controller
      name="mapperConfiguration.method"
      control={control}
      defaultValue={HashingMapperConfigurationMethod.MD5}
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
