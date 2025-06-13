import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms";
import { FormDevTools } from "components/forms/FormDevTools";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { AirbyteCatalog, DestinationCatalog, DestinationRead, SourceRead } from "core/api/types/AirbyteClient";

import { FieldMappings } from "./FieldMappings";
import { SelectDestinationObjectName } from "./SelectDestinationObjectName";
import { SelectDestinationSyncMode } from "./SelectDestinationSyncMode";
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
  const { formatMessage } = useIntl();

  const selectedSourceStream = useWatch<DataActivationConnectionFormValues, `streams.${number}.sourceStreamDescriptor`>(
    {
      name: `streams.${index}.sourceStreamDescriptor`,
    }
  );
  const selectedDestinationObject = useWatch<
    DataActivationConnectionFormValues,
    `streams.${number}.destinationObjectName`
  >({
    name: `streams.${index}.destinationObjectName`,
  });
  const sourceSyncMode = useWatch<DataActivationConnectionFormValues, `streams.${number}.sourceSyncMode`>({
    name: `streams.${index}.sourceSyncMode`,
  });
  const destinationSyncMode = useWatch<DataActivationConnectionFormValues, `streams.${number}.destinationSyncMode`>({
    name: `streams.${index}.destinationSyncMode`,
  });

  const isSourceStreamSelected = !!selectedSourceStream.name;
  const isDestinationObjectSelected = !!selectedDestinationObject;

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
              <FormControl<DataActivationConnectionFormValues>
                name={`streams.${index}.cursorField`}
                placeholder={formatMessage({ id: "form.cursorField" })}
                type="text"
                fieldType="input"
                reserveSpaceForError={false}
              />
            )}
          </div>
        )}

        {isDestinationObjectSelected && (
          <div className={styles.streamMappings__destinationSettings}>
            <SelectDestinationSyncMode streamIndex={index} destinationCatalog={destinationCatalog} />
            {destinationSyncMode === "append_dedup" && (
              <FormControl<DataActivationConnectionFormValues>
                name={`streams.${index}.primaryKey`}
                placeholder={formatMessage({ id: "connection.matchingKey" })}
                type="text"
                fieldType="input"
                reserveSpaceForError={false}
              />
            )}
          </div>
        )}

        {isSourceStreamSelected && (
          <>
            <div className={styles.streamMappings__divider} />
            <FieldMappings destinationCatalog={destinationCatalog} sourceCatalog={sourceCatalog} streamIndex={index} />
          </>
        )}
      </div>
    </Card>
  );
};
