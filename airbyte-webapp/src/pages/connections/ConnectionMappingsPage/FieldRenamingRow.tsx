import React, { useEffect, useMemo } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { fieldRenamingMapperConfiguration } from "components/connection/ConnectionForm/schemas/mapperSchema";
import { FormControlErrorMessage } from "components/forms/FormControl";
import { Text } from "components/ui/Text";

import {
  FieldRenamingMapperConfiguration,
  MapperValidationErrorType,
  StreamMapperType,
} from "core/api/types/AirbyteClient";

import { autoSubmitResolver } from "./autoSubmitResolver";
import { useMappingContext } from "./MappingContext";
import { MappingFormTextInput, MappingRowContent, MappingRowItem } from "./MappingRow";
import { MappingTypeListBox } from "./MappingTypeListBox";
import { MappingValidationErrorMessage } from "./MappingValidationErrorMessage";
import { SelectTargetField } from "./SelectTargetField";
import { StreamMapperWithId } from "./types";

interface FieldRenamingRowProps {
  mapping: StreamMapperWithId<FieldRenamingMapperConfiguration>;
  streamDescriptorKey: string;
}

export const FieldRenamingRow: React.FC<FieldRenamingRowProps> = ({ mapping, streamDescriptorKey }) => {
  const { updateLocalMapping, validatingStreams } = useMappingContext();
  const isStreamValidating = validatingStreams.has(streamDescriptorKey);

  const { formatMessage } = useIntl();

  const defaultValues = useMemo(() => {
    return {
      originalFieldName: mapping?.mapperConfiguration?.originalFieldName ?? "",
      newFieldName: mapping?.mapperConfiguration?.newFieldName ?? "",
    };
  }, [mapping]);

  const methods = useForm<FieldRenamingMapperConfiguration>({
    defaultValues,
    resolver: autoSubmitResolver(fieldRenamingMapperConfiguration, (formValues) => {
      updateLocalMapping(streamDescriptorKey, mapping.id, { mapperConfiguration: formValues });
    }),
    mode: "onBlur",
  });

  useEffect(() => {
    if (
      mapping.validationError &&
      mapping.validationError.type === MapperValidationErrorType.FIELD_NOT_FOUND &&
      "originalFieldName" in methods.formState.touchedFields
    ) {
      methods.setError("originalFieldName", {
        message: "connections.mappings.error.FIELD_NOT_FOUND",
      });
    } else {
      methods.clearErrors("originalFieldName");
    }
  }, [mapping.validationError, methods]);

  useEffect(() => {
    updateLocalMapping(streamDescriptorKey, mapping.id, { validationCallback: methods.trigger }, true);
  }, [methods.trigger, streamDescriptorKey, updateLocalMapping, mapping.id]);

  return (
    <FormProvider {...methods}>
      <form>
        <MappingRowContent>
          <MappingRowItem>
            <MappingTypeListBox
              disabled={isStreamValidating}
              selectedValue={StreamMapperType["field-renaming"]}
              streamDescriptorKey={streamDescriptorKey}
              mappingId={mapping.id}
            />
          </MappingRowItem>
          <MappingRowItem>
            <SelectTargetField<FieldRenamingMapperConfiguration>
              disabled={isStreamValidating}
              mappingId={mapping.id}
              streamDescriptorKey={streamDescriptorKey}
              name="originalFieldName"
            />
          </MappingRowItem>
          <MappingRowItem>
            <Text>
              <FormattedMessage id="connections.mappings.to" />
            </Text>
          </MappingRowItem>
          <MappingRowItem>
            <MappingFormTextInput<FieldRenamingMapperConfiguration>
              placeholder={formatMessage({ id: "connections.mappings.value" })}
              disabled={isStreamValidating}
              name="newFieldName"
            />
            <FormControlErrorMessage<FieldRenamingMapperConfiguration> name="newFieldName" />
          </MappingRowItem>
        </MappingRowContent>
        <MappingValidationErrorMessage<FieldRenamingMapperConfiguration>
          validationError={mapping.validationError}
          touchedFields={methods.formState.touchedFields}
        />
      </form>
    </FormProvider>
  );
};
