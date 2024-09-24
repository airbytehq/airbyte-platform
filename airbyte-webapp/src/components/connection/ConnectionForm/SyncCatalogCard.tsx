import React, { useCallback, useContext, useMemo, useState } from "react";
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
import { LoadingBackdrop } from "components/ui/LoadingBackdrop";
import { ScrollParentContext } from "components/ui/ScrollParent";

import { naturalComparatorBy } from "core/utils/objects";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { FormConnectionFormValues, SyncStreamFieldWithId } from "./formConfig";
import { useRefreshSourceSchemaWithConfirmationOnDirty } from "./refreshSourceSchemaWithConfirmationOnDirty";
import styles from "./SyncCatalogCard.module.scss";
import { StreamsConfigTableHeader } from "../syncCatalog/StreamsConfigTable/StreamsConfigTableHeader";
import { DisabledStreamsSwitch } from "../syncCatalog/SyncCatalog/DisabledStreamsSwitch";
import { SyncCatalogEmpty } from "../syncCatalog/SyncCatalog/SyncCatalogEmpty";
import { SyncCatalogRow } from "../syncCatalog/SyncCatalog/SyncCatalogRow";
import { SyncCatalogStreamSearch } from "../syncCatalog/SyncCatalog/SyncCatalogStreamSearch";
import { useStreamFilters } from "../syncCatalog/SyncCatalog/useStreamFilters";

interface RedirectionLocationState {
  namespace?: string;
  streamName?: string;
  action?: "showInReplicationTable" | "openDetails" | "editStream";
}

export interface LocationWithState extends Location {
  state: RedirectionLocationState;
}

export const SyncCatalogCard: React.FC = () => {
  const { mode } = useConnectionFormService();
  const { control, trigger } = useFormContext<FormConnectionFormValues>();
  const { isSubmitting, isDirty, errors } = useFormState<FormConnectionFormValues>();
  const { fields, replace, update } = useFieldArray({
    name: "syncCatalog.streams",
    control,
  });
  const watchedPrefix = useWatch<FormConnectionFormValues>({ name: "prefix", control });
  const watchedNamespaceDefinition = useWatch<FormConnectionFormValues>({ name: "namespaceDefinition", control });
  const watchedNamespaceFormat = useWatch<FormConnectionFormValues>({ name: "namespaceFormat", control });

  const [searchString, setSearchString] = useState("");
  const [hideDisabledStreams, toggleHideDisabledStreams] = useToggle(false);
  const sortedSchema = useMemo(
    () => [...fields].sort(naturalComparatorBy((syncStream) => syncStream.stream?.name ?? "")),
    [fields]
  );
  const filteredStreams = useStreamFilters(searchString, hideDisabledStreams, sortedSchema);
  const refreshSchema = useRefreshSourceSchemaWithConfirmationOnDirty(isDirty);

  const onUpdateStream = useCallback(
    ({ config, id }: SyncStreamFieldWithId) => {
      const streamNodeIndex = fields.findIndex((streamNode) => streamNode.id === id);
      update(streamNodeIndex, { ...fields[streamNodeIndex], config });

      // force validation of the form
      trigger(`syncCatalog.streams`);
    },
    [fields, trigger, update]
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

  const cardTitle = mode === "readonly" ? "connectionForm.selectStreams.readonly" : "connectionForm.selectStreams";
  const customScrollParent = useContext(ScrollParentContext);

  return (
    <Card noPadding>
      <Box p="xl" className={styles.cardHeader}>
        <FlexContainer justifyContent="space-between" alignItems="center">
          <Heading as="h2" size="sm">
            <FormattedMessage id={cardTitle} />
          </Heading>
          {mode !== "readonly" && (
            <Button
              onClick={refreshSchema}
              type="button"
              variant="secondary"
              data-testid="refresh-source-schema-btn"
              disabled={isSubmitting}
              icon="sync"
            >
              <FormattedMessage id="connection.updateSchema" />
            </Button>
          )}
        </FlexContainer>
      </Box>
      <LoadingBackdrop loading={isSubmitting}>
        <div className={styles.controlsContainer}>
          <SyncCatalogStreamSearch onSearch={setSearchString} />
          <DisabledStreamsSwitch checked={hideDisabledStreams} onChange={toggleHideDisabledStreams} />
        </div>
        <Box mb="xl" data-testid="catalog-tree-table-body">
          <StreamsConfigTableHeader
            streams={fields}
            onStreamsChanged={replace}
            syncSwitchDisabled={filteredStreams.length !== fields.length}
            namespaceDefinition={watchedNamespaceDefinition}
            namespaceFormat={watchedNamespaceFormat}
            prefix={watchedPrefix}
            headerClassName={styles.tableHeader}
          />
          {filteredStreams.length ? (
            <Virtuoso
              data={filteredStreams}
              initialTopMostItemIndex={initialTopMostItemIndex}
              fixedItemHeight={50}
              useWindowScroll
              customScrollParent={customScrollParent ?? undefined}
              itemContent={(_index, streamNode) => (
                <SyncCatalogRow
                  key={streamNode.id}
                  streamNode={streamNode}
                  updateStreamNode={onUpdateStream}
                  namespaceDefinition={watchedNamespaceDefinition}
                  namespaceFormat={watchedNamespaceFormat}
                  prefix={watchedPrefix}
                  errors={errors}
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
