import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { FormControl } from "components/forms";
import { FormDevTools } from "components/forms/FormDevTools";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
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
        <FormControl<StreamMappingsFormValues>
          name={`streams.${index}.destinationObjectName`}
          placeholder={formatMessage({ id: "connection.destinationObjectName" })}
          type="text"
          fieldType="input"
          reserveSpaceForError={false}
        />

        {isSourceStreamSelected && (
          <div className={styles.streamMappings__sourceSettings}>
            <SelectSourceSyncMode streamIndex={index} />
            {sourceSyncMode === "incremental" && (
              <FormControl<StreamMappingsFormValues>
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
            <SelectDestinationSyncMode streamIndex={index} />
            {destinationSyncMode === "append_dedup" && (
              <FormControl<StreamMappingsFormValues>
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
            <FieldMappings streamIndex={index} />
          </>
        )}
      </div>
    </Card>
  );
};

export const EMPTY_FIELD: DataActivationField = {
  sourceFieldName: "",
  destinationFieldName: "",
};

export const EMPTY_STREAM: DataActivationStream = {
  sourceStreamDescriptor: {
    name: "",
    namespace: "",
  },
  destinationObjectName: "",
  sourceSyncMode: null,
  destinationSyncMode: null,
  fields: [EMPTY_FIELD],
  primaryKey: null,
  cursorField: null,
};

interface DataActivationStream {
  sourceStreamDescriptor: {
    name: string;
    namespace: string;
  };
  destinationObjectName: string;
  sourceSyncMode: SyncMode | null;
  destinationSyncMode: DestinationSyncMode | null;
  primaryKey: string | null;
  cursorField: string | null;
  fields: DataActivationField[];
}

interface DataActivationField {
  sourceFieldName: string;
  destinationFieldName: string;
}

export interface StreamMappingsFormValues {
  streams: DataActivationStream[];
}

// This allows us to programatically set the sync mode back to null, but still validates that a user selects a sync mode
// before submitting the form.
const noSourceSyncModeSelected = z.object({
  sourceSyncMode: z.null().refine((val) => val !== null, {
    message: "form.empty.error",
  }),
});

const incrementalSyncMode = z.object({
  sourceSyncMode: z.literal(SyncMode.incremental),
  cursorField: z.string().nonempty("form.empty.error"),
});

const fullRefreshSyncMode = z.object({
  sourceSyncMode: z.literal(SyncMode.full_refresh),
  cursorField: z.null(),
});

const sourceSyncMode = z.discriminatedUnion("sourceSyncMode", [
  noSourceSyncModeSelected,
  incrementalSyncMode,
  fullRefreshSyncMode,
]);

// This allows us to programatically set the destination sync mode back to null, but still validates that a user selects
// a destination sync mode before submitting the form.
const noDestinationSyncModeSelected = z.object({
  destinationSyncMode: z.null().refine((val) => val !== null, {
    message: "form.empty.error",
  }),
});

const destinationAppendSyncMode = z.object({
  destinationSyncMode: z.literal(DestinationSyncMode.append),
  primaryKey: z.null(),
});

const destinationAppendDedupSyncMode = z.object({
  destinationSyncMode: z.literal(DestinationSyncMode.append_dedup),
  primaryKey: z.string().nonempty("form.empty.error"),
});

const destinationSyncMode = z.discriminatedUnion("destinationSyncMode", [
  noDestinationSyncModeSelected,
  destinationAppendSyncMode,
  destinationAppendDedupSyncMode,
]);

const DataActivationStreamSchema = z
  .object({
    sourceStreamDescriptor: z
      .object({
        name: z.string(),
        namespace: z.string().optional(),
      })
      .refine((s) => !!s.name && s.name.trim().length > 0, {
        message: "form.empty.error",
        path: [],
      }),
    destinationObjectName: z.string().trim().nonempty("form.empty.error"),
    fields: z.array(
      z.object({
        sourceFieldName: z.string().nonempty("form.empty.error"),
        destinationFieldName: z.string().nonempty("form.empty.error"),
      })
    ),
  })
  .and(sourceSyncMode)
  .and(destinationSyncMode);

export const StreamMappingsFormValuesSchema = z.object({
  streams: z.array(DataActivationStreamSchema),
});
