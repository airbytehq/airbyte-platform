import React, { useEffect, useState } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import {
  EncryptionMapperAESConfigurationPadding,
  EncryptionMapperAlgorithm,
  StreamMapperType,
  EncryptionMapperAESConfigurationMode,
  EncryptionMapperConfiguration,
} from "core/api/types/AirbyteClient";

import { autoSubmitResolver } from "./autoSubmitResolver";
import { useMappingContext } from "./MappingContext";
import { MappingFormTextInput } from "./MappingRow";
import styles from "./MappingRow.module.scss";
import { MappingTypeListBox } from "./MappingTypeListBox";
import { SelectFieldOption, SelectTargetField } from "./SelectTargetField";
import { StreamMapperWithId } from "./types";
import { useGetFieldsInStream } from "./useGetFieldsInStream";

const encryptionMapperSchema = yup
  .object()
  .shape({
    algorithm: yup.mixed<EncryptionMapperAlgorithm>().oneOf(Object.values(EncryptionMapperAlgorithm)).required(),
    targetField: yup.string().required("Target field is required"),
    fieldNameSuffix: yup.string().oneOf(["_encrypted"]).required("Field name suffix is required"),
    key: yup.string().when("algorithm", {
      is: "AES",
      then: yup.string().required("Key is required"),
      otherwise: yup.string().notRequired(),
    }),
    publicKey: yup.string().when("algorithm", {
      is: "RSA",
      then: yup.string().required("Public key is required"),
      otherwise: (schema) => schema.strip(),
    }),
    mode: yup.mixed<EncryptionMapperAESConfigurationMode>().when("algorithm", {
      is: "AES",
      then: yup.mixed().oneOf(Object.values(EncryptionMapperAESConfigurationMode)).required("Mode is required"),
      otherwise: (schema) => schema.strip(),
    }),
    padding: yup.mixed<EncryptionMapperAESConfigurationPadding>().when("algorithm", {
      is: "AES",
      then: yup.mixed().oneOf(Object.values(EncryptionMapperAESConfigurationPadding)).required("Padding is required"),
      otherwise: (schema) => schema.strip(),
    }),
  })
  .required();

interface EncryptionFormProps {
  streamName: string;
  mapping: StreamMapperWithId<EncryptionMapperConfiguration>;
  targetFieldOptions: SelectFieldOption[];
}

export const EncryptionForm: React.FC<EncryptionFormProps> = ({ streamName, mapping, targetFieldOptions }) => {
  const { updateLocalMapping, validateMappings } = useMappingContext();
  const { formatMessage } = useIntl();
  const [algorithm, setAlgorithm] = useState<EncryptionMapperAlgorithm>(mapping.mapperConfiguration.algorithm || "RSA");

  const methods = useForm<EncryptionMapperConfiguration>({
    defaultValues: {
      algorithm,
      targetField: mapping.mapperConfiguration.targetField ?? "",
      fieldNameSuffix: mapping.mapperConfiguration.fieldNameSuffix ?? "_encrypted",
      ...(mapping.mapperConfiguration.algorithm === "RSA" && {
        publicKey: mapping.mapperConfiguration.publicKey ?? "",
      }),

      ...(mapping.mapperConfiguration.algorithm === "AES" && {
        key: mapping.mapperConfiguration.key ?? "",
        mode: mapping.mapperConfiguration.mode ?? "CBC",
        padding: mapping.mapperConfiguration.padding ?? "PKCS5Padding",
      }),
    },
    resolver: autoSubmitResolver<EncryptionMapperConfiguration>(encryptionMapperSchema, (formValues) => {
      updateLocalMapping(streamName, mapping.id, { mapperConfiguration: formValues });
      validateMappings();
    }),
    mode: "onBlur",
  });

  useEffect(() => {
    updateLocalMapping(streamName, mapping.id, { validationCallback: methods.trigger });
  }, [methods.trigger, streamName, updateLocalMapping, mapping.id]);

  const values = methods.watch();

  return (
    <FormProvider {...methods}>
      <form className={styles.form}>
        <MappingTypeListBox
          selectedValue={StreamMapperType.encryption}
          streamName={streamName}
          mappingId={mapping.id}
        />
        <SelectTargetField<EncryptionMapperConfiguration> targetFieldOptions={targetFieldOptions} name="targetField" />
        <Text>
          <FormattedMessage id="connections.mappings.using" />
        </Text>
        <ListBox
          selectedValue={algorithm}
          onSelect={(selectedAlgorithm: EncryptionMapperAlgorithm) => {
            setAlgorithm(selectedAlgorithm);
            methods.setValue("algorithm", selectedAlgorithm);
            if (selectedAlgorithm === "AES") {
              methods.setValue("publicKey", "");
              methods.setValue("key", "");
              methods.setValue("mode", "CBC");
              methods.setValue("padding", "PKCS5Padding");
            } else if (selectedAlgorithm === "RSA") {
              methods.setValue("key", "");
              methods.setValue("publicKey", "");
            }
          }}
          options={[
            { label: "AES", value: "AES" },
            { label: "RSA", value: "RSA" },
          ]}
        />
        {values.algorithm === "AES" && (
          <>
            <Text>
              <FormattedMessage id="connections.mappings.andKey" />
            </Text>
            <MappingFormTextInput
              placeholder={formatMessage({ id: "connections.mappings.encryption.key" })}
              name="key"
            />
            <ListBox
              selectedValue={values.mode}
              onSelect={(mode: EncryptionMapperAESConfigurationMode) =>
                methods.setValue("mode", mode, { shouldValidate: true })
              }
              options={Object.values(EncryptionMapperAESConfigurationMode).map((mode) => ({
                label: mode,
                value: mode,
              }))}
            />
            <ListBox
              selectedValue={values.padding}
              onSelect={(padding: EncryptionMapperAESConfigurationPadding) =>
                methods.setValue("padding", padding, { shouldValidate: true })
              }
              options={Object.values(EncryptionMapperAESConfigurationPadding).map((padding) => ({
                label: padding,
                value: padding,
              }))}
            />
          </>
        )}
        {values.algorithm === "RSA" && (
          <>
            <Text>
              <FormattedMessage id="connections.mappings.andKey" />
            </Text>
            <MappingFormTextInput
              placeholder={formatMessage({ id: "connections.mappings.encryption.publicKey" })}
              name="publicKey"
            />
          </>
        )}
      </form>
    </FormProvider>
  );
};

export const EncryptionRow: React.FC<{
  mapping: StreamMapperWithId<EncryptionMapperConfiguration>;
  streamName: string;
}> = ({ mapping, streamName }) => {
  const fieldsInStream = useGetFieldsInStream(streamName);

  if (!mapping) {
    return null;
  }

  return <EncryptionForm streamName={streamName} mapping={mapping} targetFieldOptions={fieldsInStream} />;
};
