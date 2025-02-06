import React, { useEffect, useState } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { FormControlErrorMessage } from "components/forms/FormControl";
import { Text } from "components/ui/Text";

import {
  EncryptionMapperAlgorithm,
  StreamMapperType,
  EncryptionMapperConfiguration,
} from "core/api/types/AirbyteClient";

import { autoSubmitResolver } from "./autoSubmitResolver";
import { useMappingContext } from "./MappingContext";
import { MappingFormTextInput, MappingRowItem } from "./MappingRow";
import styles from "./MappingRow.module.scss";
import { MappingTypeListBox } from "./MappingTypeListBox";
import { SelectTargetField } from "./SelectTargetField";
import { StreamMapperWithId } from "./types";

const isHexadecimal = (value: string | undefined) => (value ? /^[0-9a-fA-F]*$/.test(value) : false);

/**
 * AES is still _technically_ supported in the API but in reality, the secrets hydration is broken
 * It should not be supported in the UI
 * https://github.com/airbytehq/airbyte-internal-issues/issues/11515
 */
const encryptionMapperSchema = yup
  .object()
  .shape({
    algorithm: yup.mixed<EncryptionMapperAlgorithm>().oneOf([EncryptionMapperAlgorithm.RSA]).required(),
    targetField: yup.string().required("Target field is required"),
    fieldNameSuffix: yup.string().oneOf(["_encrypted"]).required("Field name suffix is required"),
    publicKey: yup.string().when("algorithm", {
      is: "RSA",
      then: yup
        .string()
        .required("Public key is required")
        .test("is-hex", "Public key must be in hexadecimal format", isHexadecimal),
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
  const [algorithm] = useState<EncryptionMapperAlgorithm>(
    mapping.mapperConfiguration.algorithm || EncryptionMapperAlgorithm.RSA
  );

  const methods = useForm<EncryptionMapperConfiguration>({
    defaultValues: {
      algorithm,
      targetField: mapping.mapperConfiguration.targetField ?? "",
      fieldNameSuffix: mapping.mapperConfiguration.fieldNameSuffix ?? "_encrypted",
      ...(mapping.mapperConfiguration.algorithm === EncryptionMapperAlgorithm.RSA && {
        publicKey: mapping.mapperConfiguration.publicKey ?? "",
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
            <FormattedMessage id="connections.mappings.usingRsaAndKey" />
          </Text>
        </MappingRowItem>
        {values.algorithm === "RSA" && (
          <MappingRowItem>
            <MappingFormTextInput
              placeholder={formatMessage({ id: "connections.mappings.encryption.publicKey" })}
              name="publicKey"
              disabled={isStreamValidating}
            />
            <FormControlErrorMessage<EncryptionMapperConfiguration> name="publicKey" />
          </MappingRowItem>
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
