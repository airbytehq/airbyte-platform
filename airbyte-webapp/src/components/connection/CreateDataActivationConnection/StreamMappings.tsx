import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { FormDevTools } from "components/forms/FormDevTools";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

import { FieldMappings } from "./FieldMappings";
import { SelectDestinationSyncMode } from "./SelectDestinationSyncMode";
import { SelectSourceStream } from "./SelectSourceStream";
import { SelectSourceSyncMode } from "./SelectSourceSyncMode";
import styles from "./StreamMappings.module.scss";

export type StreamMappingsFormValuesType = z.infer<typeof StreamMappingsFormValuesSchema>;

export const StreamMappings = () => {
  const { control } = useFormContext<StreamMappingsFormValues>();
  const { fields: streams } = useFieldArray<StreamMappingsFormValues>({
    control,
    name: "streams",
  });

  return (
    <Box pb="xl" mb="xl">
      {streams.map((field, index) => {
        return (
          <Box key={field.id} mb="lg">
            <FlexContainer gap="lg">
              <FlexItem grow>
                <StreamMapping index={index} />
              </FlexItem>
            </FlexContainer>
          </Box>
        );
      })}
      <FormDevTools />
    </Box>
  );
};

const StreamMapping = ({ index }: { index: number }) => {
  const { register } = useFormContext<StreamMappingsFormValues>();
  const { formatMessage } = useIntl();

  const selectedSourceStream = useWatch<StreamMappingsFormValues, `streams.${number}.sourceStreamDescriptor`>({
    name: `streams.${index}.sourceStreamDescriptor`,
  });
  const selectedDestinationObject = useWatch<StreamMappingsFormValues, `streams.${number}.destinationObjectName`>({
    name: `streams.${index}.destinationObjectName`,
  });
  const sourceSyncMode = useWatch<StreamMappingsFormValues, `streams.${number}.sourceSyncMode`>({
    name: `streams.${index}.sourceSyncMode`,
  });
  const destinationSyncMode = useWatch<StreamMappingsFormValues, `streams.${number}.destinationSyncMode`>({
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
        <SelectSourceStream index={index} />
        <div className={styles.streamMappings__arrow}>
          <Icon type="arrowRight" size="lg" color="action" />
        </div>
        <Input
          {...register(`streams.${index}.destinationObjectName`)}
          placeholder={formatMessage({ id: "connection.destinationObjectName" })}
        />

        {isSourceStreamSelected && (
          <div className={styles.streamMappings__sourceSettings}>
            <SelectSourceSyncMode streamIndex={index} />
            {sourceSyncMode === "incremental" && (
              <Input
                {...register(`streams.${index}.cursorField`)}
                placeholder={formatMessage({ id: "form.cursorField" })}
              />
            )}
          </div>
        )}

        {isDestinationObjectSelected && (
          <div className={styles.streamMappings__destinationSettings}>
            <SelectDestinationSyncMode streamIndex={index} />
            {destinationSyncMode === "append_dedup" && (
              <Input
                {...register(`streams.${index}.primaryKey`)}
                placeholder={formatMessage({ id: "connection.matchingKey" })}
              />
            )}
          </div>
        )}

        {isSourceStreamSelected && (
          <>
            <div className={styles.streamMappings__divider} />
            <FieldMappings streamIndex={index} />
          </>
        )}
      </div>
    </Card>
  );
};

export const EMPTY_STREAM: DataActivationStream = {
  sourceStreamDescriptor: {
    name: "",
    namespace: "",
  },
  destinationObjectName: "",
  sourceSyncMode: undefined,
  destinationSyncMode: undefined,
  fields: [],
};

interface DataActivationStream {
  sourceStreamDescriptor: {
    name: string;
    namespace: string;
  };
  destinationObjectName: string;
  sourceSyncMode?: SyncMode;
  destinationSyncMode?: DestinationSyncMode;
  primaryKey?: string;
  cursorField?: string;
  fields: Array<{ sourceFieldName: string; destinationFieldName: string }>;
}

export interface StreamMappingsFormValues {
  streams: DataActivationStream[];
}

const DataActivationStreamSchema = z.object({
  sourceStreamDescriptor: z.object({
    name: z.string(),
    namespace: z.string().optional(),
  }),
  destinationObjectName: z.string(),
  sourceSyncMode: z.nativeEnum(SyncMode),
  destinationSyncMode: z.nativeEnum(DestinationSyncMode),
  primaryKey: z.string().optional(),
  cursorField: z.string().optional(),
  fields: z.array(
    z.object({
      sourceFieldName: z.string(),
      destinationFieldName: z.string(),
    })
  ),
});

export const StreamMappingsFormValuesSchema = z.object({
  streams: z.array(DataActivationStreamSchema),
});
