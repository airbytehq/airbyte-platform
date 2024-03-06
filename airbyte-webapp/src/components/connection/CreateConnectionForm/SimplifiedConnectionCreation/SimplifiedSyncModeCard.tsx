import { useMemo, useState } from "react";
import { useIntl } from "react-intl";

import { SUPPORTED_MODES } from "components/connection/ConnectionForm/formConfig";
import { FormFieldLayout } from "components/connection/ConnectionForm/FormFieldLayout";
import { SyncModeValue } from "components/connection/syncCatalog/SyncModeSelect";
import { ControlLabels } from "components/LabeledControl";
import { FlexContainer } from "components/ui/Flex";
import { ListBox, Option } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

export const SimplifiedSyncModeCard = () => {
  const { formatMessage } = useIntl();
  const {
    connection,
    destDefinitionSpecification: { supportedDestinationSyncModes },
  } = useConnectionFormService();

  const streamSupportedSyncModes: SyncMode[] = useMemo(() => {
    const foundModes = new Set<SyncMode>();
    for (let i = 0; i < connection.syncCatalog.streams.length; i++) {
      const stream = connection.syncCatalog.streams[i];
      stream.stream?.supportedSyncModes?.forEach((mode) => foundModes.add(mode));
    }
    return Array.from(foundModes);
  }, [connection.syncCatalog.streams]);

  const availableSyncModes: SyncModeValue[] = useMemo(
    () =>
      SUPPORTED_MODES.filter(
        ([syncMode, destinationSyncMode]) =>
          streamSupportedSyncModes.includes(syncMode) && supportedDestinationSyncModes?.includes(destinationSyncMode)
      ).map(([syncMode, destinationSyncMode]) => ({
        syncMode,
        destinationSyncMode,
      })),
    [streamSupportedSyncModes, supportedDestinationSyncModes]
  );

  const syncModeOptions: Array<Option<SyncModeValue>> = useMemo(
    () =>
      availableSyncModes.map((option) => {
        const syncModeId = option.syncMode === SyncMode.full_refresh ? "syncMode.fullRefresh" : "syncMode.incremental";
        const destinationSyncModeId =
          option.destinationSyncMode === DestinationSyncMode.overwrite
            ? "destinationSyncMode.overwrite"
            : option.destinationSyncMode === DestinationSyncMode.append_dedup
            ? "destinationSyncMode.appendDedup"
            : "destinationSyncMode.append";
        return {
          label: `${formatMessage({ id: syncModeId })} | ${formatMessage({
            id: destinationSyncModeId,
          })}`,
          value: option,
        };
      }),
    [formatMessage, availableSyncModes]
  );

  const [defaultSyncMode, setDefaultSyncMode] = useState<SyncModeValue | null>(availableSyncModes[0]);

  return (
    <FormFieldLayout nextSizing>
      <ControlLabels
        label={
          <FlexContainer direction="column">
            <Text bold>{formatMessage({ id: "form.syncMode" })}</Text>
            <Text size="sm" color="grey">
              {formatMessage({ id: "form.syncMode.subtitle" })}
            </Text>
          </FlexContainer>
        }
      />
      <ListBox selectedValue={defaultSyncMode} options={syncModeOptions} onSelect={setDefaultSyncMode} />
    </FormFieldLayout>
  );
};
