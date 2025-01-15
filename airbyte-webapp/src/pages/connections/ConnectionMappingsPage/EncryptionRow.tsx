import React, { useEffect, useState } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { FormControlErrorMessage } from "components/forms/FormControl";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip/Tooltip";

import {
  EncryptionMapperAESConfigurationPadding,
  EncryptionMapperAlgorithm,
  StreamMapperType,
  EncryptionMapperAESConfigurationMode,
  EncryptionMapperConfiguration,
} from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { useExperiment } from "hooks/services/Experiment";

import { autoSubmitResolver } from "./autoSubmitResolver";
import { useMappingContext } from "./MappingContext";
import { MappingFormTextInput, MappingRowItem } from "./MappingRow";
import styles from "./MappingRow.module.scss";
import { MappingTypeListBox } from "./MappingTypeListBox";
import { SelectTargetField } from "./SelectTargetField";
import { StreamMapperWithId } from "./types";

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
  streamDescriptorKey: string;
  mapping: StreamMapperWithId<EncryptionMapperConfiguration>;
}

export const EncryptionForm: React.FC<EncryptionFormProps> = ({ streamDescriptorKey, mapping }) => {
  const { updateLocalMapping, validatingStreams } = useMappingContext();
  const isStreamValidating = validatingStreams.has(streamDescriptorKey);
  const { formatMessage } = useIntl();
  const [algorithm, setAlgorithm] = useState<EncryptionMapperAlgorithm>(mapping.mapperConfiguration.algorithm || "RSA");
  const isSecretsPersistenceEnabled = useExperiment("platform.use-runtime-secret-persistence");
  const isEnterprise = useFeature(FeatureItem.EnterpriseBranding);
  const enableAES = isEnterprise || isSecretsPersistenceEnabled;

  const methods = useForm<EncryptionMapperConfiguration>({
    defaultValues: {
      algorithm,
      targetField: mapping.mapperConfiguration.targetField ?? "",
      fieldNameSuffix: mapping.mapperConfiguration.fieldNameSuffix ?? "_encrypted",
      ...(mapping.mapperConfiguration.algorithm === "RSA" && {
        publicKey: mapping.mapperConfiguration.publicKey ?? "",
      }),
      ...(mapping.mapperConfiguration.algorithm === "AES" && {
        targetField: mapping.mapperConfiguration.targetField ?? "",
        fieldNameSuffix: mapping.mapperConfiguration.fieldNameSuffix ?? "_encrypted",
      }),
    },
    resolver: autoSubmitResolver<EncryptionMapperConfiguration>(encryptionMapperSchema, (formValues) => {
      updateLocalMapping(streamDescriptorKey, mapping.id, { mapperConfiguration: formValues });
    }),
    mode: "onBlur",
  });

  useEffect(() => {
    if (mapping.validationError && mapping.validationError.type === "FIELD_NOT_FOUND") {
      methods.setError("targetField", { message: mapping.validationError.message });
    } else {
      methods.clearErrors("targetField");
    }
  }, [mapping.validationError, methods]);

  useEffect(() => {
    updateLocalMapping(streamDescriptorKey, mapping.id, { validationCallback: methods.trigger }, true);
  }, [methods.trigger, streamDescriptorKey, updateLocalMapping, mapping.id]);

  const values = methods.watch();

  return (
    <FormProvider {...methods}>
      <form className={styles.form}>
        <MappingRowItem>
          <MappingTypeListBox
            disabled={isStreamValidating}
            selectedValue={StreamMapperType.encryption}
            streamDescriptorKey={streamDescriptorKey}
            mappingId={mapping.id}
          />
        </MappingRowItem>
        <MappingRowItem>
          <SelectTargetField<EncryptionMapperConfiguration>
            mappingId={mapping.id}
            streamDescriptorKey={streamDescriptorKey}
            name="targetField"
            disabled={isStreamValidating}
          />
        </MappingRowItem>
        <MappingRowItem>
          <Text>
            <FormattedMessage id="connections.mappings.using" />
          </Text>
        </MappingRowItem>
        <MappingRowItem>
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
              { label: "RSA", value: "RSA" },
              {
                label: enableAES ? (
                  "AES"
                ) : (
                  <Tooltip control="AES">
                    <FormattedMessage id="connections.mappings.aesRequiresSecrets" />
                  </Tooltip>
                ),
                value: "AES",
                disabled: !enableAES,
              },
            ]}
          />
        </MappingRowItem>

        {values.algorithm === "AES" && (
          <>
            <MappingRowItem>
              <Text>
                <FormattedMessage id="connections.mappings.andKey" />
              </Text>
            </MappingRowItem>
            <MappingRowItem>
              <MappingFormTextInput
                placeholder={formatMessage({ id: "connections.mappings.encryption.key" })}
                name="key"
                disabled={isStreamValidating}
              />
              <FormControlErrorMessage<EncryptionMapperConfiguration> name="key" />
            </MappingRowItem>
            <MappingRowItem>
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
            </MappingRowItem>
            <MappingRowItem>
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
            </MappingRowItem>
          </>
        )}
        {values.algorithm === "RSA" && (
          <>
            <MappingRowItem>
              <Text>
                <FormattedMessage id="connections.mappings.andKey" />
              </Text>
            </MappingRowItem>
            <MappingRowItem>
              <MappingFormTextInput
                placeholder={formatMessage({ id: "connections.mappings.encryption.publicKey" })}
                name="publicKey"
                disabled={isStreamValidating}
              />
              <FormControlErrorMessage<EncryptionMapperConfiguration> name="publicKey" />
            </MappingRowItem>
          </>
        )}
        {mapping.validationError && mapping.validationError.type !== "FIELD_NOT_FOUND" && (
          <Text italicized color="red">
            {mapping.validationError.message}
          </Text>
        )}
      </form>
    </FormProvider>
  );
};

export const EncryptionRow: React.FC<{
  mapping: StreamMapperWithId<EncryptionMapperConfiguration>;
  streamDescriptorKey: string;
}> = ({ mapping, streamDescriptorKey }) => {
  if (!mapping) {
    return null;
  }

  return <EncryptionForm streamDescriptorKey={streamDescriptorKey} mapping={mapping} />;
};
