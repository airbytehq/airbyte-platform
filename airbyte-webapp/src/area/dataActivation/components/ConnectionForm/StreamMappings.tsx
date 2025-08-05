import { useMemo } from "react";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { FormDevTools } from "components/forms/FormDevTools";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { AirbyteCatalog, DestinationCatalog, DestinationRead, SourceRead } from "core/api/types/AirbyteClient";

import { FieldMapping } from "./FieldMapping";
import { SelectCursorField } from "./SelectCursorField";
import { SelectDestinationObjectName } from "./SelectDestinationObjectName";
import { SelectDestinationSyncMode } from "./SelectDestinationSyncMode";
import { SelectMatchingKey } from "./SelectMatchingKeys";
import { SelectSourceStream } from "./SelectSourceStream";
import { SelectSourceSyncMode } from "./SelectSourceSyncMode";
import styles from "./StreamMappings.module.scss";

interface StreamMappingsProps {
  destination: DestinationRead;
  destinationCatalog: DestinationCatalog;
  source: SourceRead;
  sourceCatalog: AirbyteCatalog;
}

export const StreamMappings: React.FC<StreamMappingsProps> = ({
  destination,
  destinationCatalog,
  source,
  sourceCatalog,
}) => {
  const { control } = useFormContext<DataActivationConnectionFormValues>();
  const { fields: streams } = useFieldArray<DataActivationConnectionFormValues>({
    control,
    name: "streams",
  });

  return (
    <Box pb="xl" mb="xl" className={styles.streamMappings__container}>
      {streams.map((field, index) => {
        return (
          <Box key={field.id} mb="lg">
            <FlexContainer gap="lg">
              <FlexItem grow>
                <StreamMapping
                  index={index}
                  destination={destination}
                  destinationCatalog={destinationCatalog}
                  source={source}
                  sourceCatalog={sourceCatalog}
                />
              </FlexItem>
            </FlexContainer>
          </Box>
        );
      })}
      <FormDevTools />
    </Box>
  );
};

interface StreamMappingProps {
  index: number;
  destination: DestinationRead;
  destinationCatalog: DestinationCatalog;
  source: SourceRead;
  sourceCatalog: AirbyteCatalog;
}

const StreamMapping: React.FC<StreamMappingProps> = ({
  index,
  destination,
  destinationCatalog,
  source,
  sourceCatalog,
}) => {
  const {
    fields,
    append: appendField,
    remove: removeField,
  } = useFieldArray<DataActivationConnectionFormValues, `streams.${number}.fields`>({
    name: `streams.${index}.fields`,
  });

  const sourceStreamDescriptor = useWatch<
    DataActivationConnectionFormValues,
    `streams.${number}.sourceStreamDescriptor`
  >({
    name: `streams.${index}.sourceStreamDescriptor`,
  });
  const destinationObjectName = useWatch<DataActivationConnectionFormValues, `streams.${number}.destinationObjectName`>(
    {
      name: `streams.${index}.destinationObjectName`,
    }
  );
  const sourceSyncMode = useWatch<DataActivationConnectionFormValues, `streams.${number}.sourceSyncMode`>({
    name: `streams.${index}.sourceSyncMode`,
  });
  const destinationSyncMode = useWatch<DataActivationConnectionFormValues, `streams.${number}.destinationSyncMode`>({
    name: `streams.${index}.destinationSyncMode`,
  });

  const isSourceStreamSelected = !!sourceStreamDescriptor.name;
  const isDestinationObjectSelected = !!destinationObjectName;

  const selectedDestinationOperation = useMemo(() => {
    return destinationCatalog.operations.find(
      (operation) => operation.objectName === destinationObjectName && operation.syncMode === destinationSyncMode
    );
  }, [destinationCatalog.operations, destinationObjectName, destinationSyncMode]);

  return (
    <Card>
      <div className={styles.streamMappings}>
        <FlexContainer className={styles.streamMappings__leftGutter} alignItems="center">
          <Text size="lg">
            <FormattedMessage id="connection.create.map" />
          </Text>
        </FlexContainer>
        <SelectSourceStream index={index} source={source} sourceCatalog={sourceCatalog} />
        <div className={styles.streamMappings__arrow}>
          <Icon type="arrowRight" size="lg" color="action" />
        </div>
        <SelectDestinationObjectName
          destination={destination}
          destinationCatalog={destinationCatalog}
          streamIndex={index}
        />

        {isSourceStreamSelected && (
          <div className={styles.streamMappings__sourceSettings}>
            <SelectSourceSyncMode streamIndex={index} sourceCatalog={sourceCatalog} />
            {sourceSyncMode === "incremental" && (
              <SelectCursorField sourceCatalog={sourceCatalog} streamIndex={index} />
            )}
          </div>
        )}

        {isDestinationObjectSelected && (
          <div className={styles.streamMappings__destinationSettings}>
            <SelectDestinationSyncMode streamIndex={index} destinationCatalog={destinationCatalog} />
            {selectedDestinationOperation?.matchingKeys && selectedDestinationOperation.matchingKeys.length > 0 && (
              <SelectMatchingKey
                destinationCatalog={destinationCatalog}
                streamIndex={index}
                appendField={appendField}
              />
            )}
          </div>
        )}

        {isSourceStreamSelected && (
          <>
            <div className={styles.streamMappings__divider} />
            {fields.map((field, fieldIndex) => (
              <FieldMapping
                destinationCatalog={destinationCatalog}
                sourceCatalog={sourceCatalog}
                key={field.id}
                streamIndex={index}
                fieldIndex={fieldIndex}
                removeField={fields.length > 1 ? () => removeField(fieldIndex) : undefined}
              />
            ))}
            <Box py="sm" className={styles.streamMappings__addField}>
              <Button
                icon="plus"
                variant="secondary"
                type="button"
                onClick={() => appendField({ sourceFieldName: "", destinationFieldName: "" })}
              >
                <FormattedMessage id="connection.create.addField" />
              </Button>
            </Box>
          </>
        )}
      </div>
    </Card>
  );
};

export const useSelectedSourceStream = (sourceCatalog: AirbyteCatalog, streamIndex: number) => {
  const sourceStreamDescriptor = useWatch<
    DataActivationConnectionFormValues,
    `streams.${number}.sourceStreamDescriptor`
  >({
    name: `streams.${streamIndex}.sourceStreamDescriptor`,
  });
  return useMemo(
    () =>
      sourceCatalog.streams.find(
        (stream) =>
          stream.stream?.name === sourceStreamDescriptor.name &&
          stream.stream?.namespace === sourceStreamDescriptor.namespace
      ),
    [sourceCatalog, sourceStreamDescriptor]
  );
};
