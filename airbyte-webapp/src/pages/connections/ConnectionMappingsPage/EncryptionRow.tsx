import React, { useEffect, useState } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { encryptionMapperSchema } from "components/connection/ConnectionForm/schemas/mapperSchema";
import { FormControlErrorMessage } from "components/forms/FormControl";
import { Text } from "components/ui/Text";

import {
  EncryptionMapperAlgorithm,
  StreamMapperType,
  EncryptionMapperConfiguration,
  MapperValidationErrorType,
} from "core/api/types/AirbyteClient";

import { autoSubmitResolver } from "./autoSubmitResolver";
import { useMappingContext } from "./MappingContext";
import { MappingFormTextInput, MappingRowContent, MappingRowItem } from "./MappingRow";
import { MappingTypeListBox } from "./MappingTypeListBox";
import { MappingValidationErrorMessage } from "./MappingValidationErrorMessage";
import { SelectTargetField } from "./SelectTargetField";
import { StreamMapperWithId } from "./types";

// /**
//  * AES is still _technically_ supported in the API but in reality, the secrets hydration is broken
//  * It should not be supported in the UI
//  * https://github.com/airbytehq/airbyte-internal-issues/issues/11515
//  */

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
    resolver: autoSubmitResolver(encryptionMapperSchema, (formValues) => {
      updateLocalMapping(streamDescriptorKey, mapping.id, { mapperConfiguration: formValues });
    }),
    mode: "onBlur",
  });

  useEffect(() => {
    if (
      mapping.validationError &&
      mapping.validationError.type === MapperValidationErrorType.FIELD_NOT_FOUND &&
      "targetField" in methods.formState.touchedFields
    ) {
      methods.setError("targetField", {
        message: "connections.mappings.error.FIELD_NOT_FOUND",
      });
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
      <form>
        <MappingRowContent>
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
        </MappingRowContent>
        <MappingValidationErrorMessage<EncryptionMapperConfiguration>
          validationError={mapping.validationError}
          touchedFields={methods.formState.touchedFields}
        />
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
