import { useEffect, useMemo } from "react";
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
import { MappingRowContent, MappingRowItem } from "./MappingRow";
import styles from "./MappingRow.module.scss";
import { MappingTypeListBox } from "./MappingTypeListBox";
import { SelectTargetField } from "./SelectTargetField";
import { StreamMapperWithId } from "./types";
import { useGetFieldsInStream } from "./useGetFieldsInStream";

const hashingMapperConfigSchema = yup.object().shape({
  targetField: yup.string().required("form.empty.error"),
  method: yup
    .mixed<HashingMapperConfigurationMethod>()
    .oneOf(Object.values(HashingMapperConfigurationMethod))
    .required("form.empty.error"),
  fieldNameSuffix: yup.string().required("form.empty.error"),
});

export const HashFieldRow: React.FC<{
  mapping: StreamMapperWithId<HashingMapperConfiguration>;
  streamDescriptorKey: string;
}> = ({ mapping, streamDescriptorKey }) => {
  const { updateLocalMapping, validateMappings } = useMappingContext();
  const fieldsInStream = useGetFieldsInStream(streamDescriptorKey);

  const defaultValues = useMemo(() => {
    return {
      targetField: mapping.mapperConfiguration.targetField ?? "",
      method: mapping.mapperConfiguration.method ?? HashingMapperConfigurationMethod.MD5,
      fieldNameSuffix: mapping.mapperConfiguration.fieldNameSuffix ?? "_hashed",
    };
  }, [mapping]);

  const methods = useForm<HashingMapperConfiguration>({
    defaultValues,
    resolver: autoSubmitResolver<HashingMapperConfiguration>(hashingMapperConfigSchema, (formValues) => {
      updateLocalMapping(streamDescriptorKey, mapping.id, { mapperConfiguration: formValues });
      validateMappings();
    }),
    mode: "onBlur",
  });

  useEffect(() => {
    updateLocalMapping(streamDescriptorKey, mapping.id, { validationCallback: methods.trigger });
  }, [methods.trigger, streamDescriptorKey, updateLocalMapping, mapping.id]);

  if (!mapping) {
    return null;
  }

  return (
    <FormProvider {...methods}>
      <form>
        <MappingRowContent>
          <MappingTypeListBox
            selectedValue={StreamMapperType.hashing}
            mappingId={mapping.id}
            streamDescriptorKey={streamDescriptorKey}
          />
          <SelectTargetField<HashingMapperConfiguration> name="targetField" targetFieldOptions={fieldsInStream} />
          <MappingRowItem>
            <Text>
              <FormattedMessage id="connections.mappings.using" />
            </Text>
          </MappingRowItem>
          <MappingRowItem>
            <SelectHashingMethod />
          </MappingRowItem>
        </MappingRowContent>
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
  const { control } = useFormContext<HashingMapperConfiguration>();

  return (
    <Controller
      name="method"
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
