import classnames from "classnames";
import React, { useCallback, useMemo, useRef, useState } from "react";
import { useFieldArray, useFormContext, useFormState, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { Location, useLocation } from "react-router-dom";
import { useToggle } from "react-use";
import { IndexLocationWithAlign, Virtuoso } from "react-virtuoso";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { LoadingBackdrop } from "components/ui/LoadingBackdrop";

import { naturalComparatorBy } from "core/utils/objects";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { HookFormConnectionFormValues, SyncStreamFieldWithId } from "./hookFormConfig";
import { useRefreshSourceSchemaWithConfirmationOnDirty } from "./refreshSourceSchemaWithConfirmationOnDirty";
import styles from "./SyncCatalogHookFormField.module.scss";
import { StreamsConfigTableHeaderHookForm } from "../syncCatalog/StreamsConfigTable/StreamsConfigTableHeaderHookForm";
import { DisabledStreamsSwitch } from "../syncCatalog/SyncCatalog/DisabledStreamsSwitch";
import { LocationWithState } from "../syncCatalog/SyncCatalog/SyncCatalogBody";
import { SyncCatalogEmpty } from "../syncCatalog/SyncCatalog/SyncCatalogEmpty";
import { SyncCatalogRowHookForm } from "../syncCatalog/SyncCatalog/SyncCatalogRowHookForm";
import { SyncCatalogStreamSearch } from "../syncCatalog/SyncCatalog/SyncCatalogStreamSearch";
import { useStreamFiltersHookForm } from "../syncCatalog/SyncCatalog/useStreamFiltersHookForm";

interface RedirectionLocationStateHookForm {
  namespace?: string;
  streamName?: string;
  action?: "showInReplicationTable" | "openDetails";
}

export interface LocationWithStateHookForm extends Location {
  state: RedirectionLocationStateHookForm;
}

/**
 * react-hook-form sync catalog field component
 */
export const SyncCatalogHookFormField: React.FC = () => {
  const listRef = useRef<HTMLElement | Window | null>(null);
  const { mode } = useConnectionFormService();
  const { control, trigger, setValue } = useFormContext<HookFormConnectionFormValues>();
  const { isSubmitting, isDirty, errors } = useFormState<HookFormConnectionFormValues>();
  const { fields, replace } = useFieldArray({
    name: "syncCatalog.streams",
    control,
  });

  const watchedPrefix = useWatch<HookFormConnectionFormValues>({ name: "prefix", control });
  const watchedNamespaceDefinition = useWatch<HookFormConnectionFormValues>({ name: "namespaceDefinition", control });
  const watchedNamespaceFormat = useWatch<HookFormConnectionFormValues>({ name: "namespaceFormat", control });

  const [searchString, setSearchString] = useState("");
  const [hideDisabledStreams, toggleHideDisabledStreams] = useToggle(false);
  const sortedSchema = useMemo(
    () => [...fields].sort(naturalComparatorBy((syncStream) => syncStream.stream?.name ?? "")),
    [fields]
  );
  const filteredStreams = useStreamFiltersHookForm(searchString, hideDisabledStreams, sortedSchema);
  const refreshSchema = useRefreshSourceSchemaWithConfirmationOnDirty(isDirty);

  const onUpdateStream = useCallback(
    ({ config, id }: SyncStreamFieldWithId) => {
      const streamNodeIndex = fields.findIndex((streamNode) => streamNode.id === id);

      // TODO: Replace "setValue()" with "update()" when we fix the issue https://github.com/airbytehq/airbyte/issues/31820
      setValue(`syncCatalog.streams.${streamNodeIndex}.config`, config, {
        shouldDirty: true,
      });
      // force validation of the form
      trigger(`syncCatalog.streams`);
    },
    [fields, setValue, trigger]
  );

  // Scroll to the stream that was redirected from the Status tab
  const { state: locationState } = useLocation() as LocationWithState;
  const initialTopMostItemIndex: IndexLocationWithAlign | undefined = useMemo(() => {
    if (locationState?.action !== "showInReplicationTable" && locationState?.action !== "openDetails") {
      return;
    }

    return {
      index: filteredStreams.findIndex(
        (stream) =>
          stream.stream?.name === locationState?.streamName && stream.stream?.namespace === locationState?.namespace
      ),
      align: "center",
    };
  }, [locationState?.action, locationState?.namespace, locationState?.streamName, filteredStreams]);

  return (
    <Card>
      <FlexContainer justifyContent="space-between" alignItems="center" className={styles.header}>
        <Heading as="h2" size="sm">
          <FormattedMessage id={mode === "readonly" ? "form.dataSync.readonly" : "form.dataSync"} />
        </Heading>
        {mode !== "readonly" && (
          <Button
            onClick={refreshSchema}
            type="button"
            variant="secondary"
            data-testid="refresh-source-schema-btn"
            disabled={isSubmitting}
            icon={<Icon type="sync" />}
          >
            <FormattedMessage id="connection.updateSchema" />
          </Button>
        )}
      </FlexContainer>
      <LoadingBackdrop loading={isSubmitting}>
        <SyncCatalogStreamSearch onSearch={setSearchString} />
        <DisabledStreamsSwitch checked={hideDisabledStreams} onChange={toggleHideDisabledStreams} />
        <Box mb="xl" data-testid="catalog-tree-table-body">
          <StreamsConfigTableHeaderHookForm
            streams={fields}
            onStreamsChanged={replace}
            syncSwitchDisabled={filteredStreams.length !== fields.length}
            namespaceDefinition={watchedNamespaceDefinition}
            namespaceFormat={watchedNamespaceFormat}
            prefix={watchedPrefix}
          />
          {filteredStreams.length ? (
            <Virtuoso
              // need to set exact height
              style={{ height: "40vh" }}
              scrollerRef={(ref) => (listRef.current = ref)}
              data={filteredStreams}
              initialTopMostItemIndex={initialTopMostItemIndex}
              fixedItemHeight={50}
              itemContent={(_index, streamNode) => (
                <SyncCatalogRowHookForm
                  key={streamNode.id}
                  streamNode={streamNode}
                  updateStreamNode={onUpdateStream}
                  namespaceDefinition={watchedNamespaceDefinition}
                  namespaceFormat={watchedNamespaceFormat}
                  prefix={watchedPrefix}
                  errors={errors}
                  className={classnames({
                    [styles.withScrollbar]:
                      listRef.current instanceof HTMLDivElement &&
                      listRef?.current?.clientHeight < listRef?.current?.scrollHeight,
                  })}
                />
              )}
            />
          ) : (
            <SyncCatalogEmpty
              customText={filteredStreams.length !== fields.length ? "connection.catalogTree.noMatchingStreams" : ""}
            />
          )}
        </Box>
      </LoadingBackdrop>
    </Card>
  );
};
