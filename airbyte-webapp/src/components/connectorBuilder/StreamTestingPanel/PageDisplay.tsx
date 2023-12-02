import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Pre } from "components/ui/Pre";
import { Text } from "components/ui/Text";
import { InfoTooltip, Tooltip } from "components/ui/Tooltip";

import {
  StreamReadInferredSchema,
  StreamReadSlicesItemPagesItem,
  StreamReadSlicesItemPagesItemRecordsItem,
} from "core/api/types/ConnectorBuilderClient";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useConnectorBuilderTestRead } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./PageDisplay.module.scss";
import { RecordTable } from "./RecordTable";
import { SchemaDiffView } from "./SchemaDiffView";
import { TabData, TabbedDisplay } from "./TabbedDisplay";
import { SchemaConflictIndicator } from "../SchemaConflictIndicator";
import { useBuilderWatch } from "../types";
import { useAutoImportSchema } from "../useAutoImportSchema";
import { formatForDisplay, formatJson } from "../utils";

interface PageDisplayProps {
  page: StreamReadSlicesItemPagesItem;
  inferredSchema?: StreamReadInferredSchema;
  className?: string;
}

export const PageDisplay: React.FC<PageDisplayProps> = ({ page, className, inferredSchema }) => {
  const { formatMessage } = useIntl();

  const mode = useBuilderWatch("mode");
  const testStreamIndex = useBuilderWatch("testStreamIndex");
  const {
    streamRead,
    schemaWarnings: { incompatibleSchemaErrors, schemaDifferences },
  } = useConnectorBuilderTestRead();

  const autoImportSchema = useAutoImportSchema(testStreamIndex);

  const formattedRequest = useMemo(() => formatForDisplay(page.request), [page.request]);
  const formattedResponse = useMemo(() => formatForDisplay(page.response), [page.response]);

  const tabs: TabData[] = [
    {
      key: "records",
      title: (
        <>
          {`${formatMessage({ id: "connectorBuilder.recordsTab" })} (${page.records.length})`}
          {!streamRead.isFetching && streamRead.data && streamRead.data.test_read_limit_reached && (
            <InfoTooltip>
              <FormattedMessage id="connectorBuilder.streamTestLimitReached" />
            </InfoTooltip>
          )}
        </>
      ),
      content: <RecordDisplay records={page.records} />,
    },
    ...(page.request
      ? [
          {
            key: "request",
            title: <FormattedMessage id="connectorBuilder.requestTab" />,
            content: <Pre>{formattedRequest}</Pre>,
          },
        ]
      : []),
    ...(page.response
      ? [
          {
            key: "response",
            title: <FormattedMessage id="connectorBuilder.responseTab" />,
            content: <Pre>{formattedResponse}</Pre>,
          },
        ]
      : []),
    ...(inferredSchema
      ? [
          {
            key: "schema",
            title: (
              <FlexContainer direction="row" justifyContent="center">
                <FormattedMessage id="connectorBuilder.schemaTab" />
                {mode === "ui" && schemaDifferences && !autoImportSchema && (
                  <SchemaConflictIndicator errors={incompatibleSchemaErrors} />
                )}
              </FlexContainer>
            ),
            content: (
              <SchemaDiffView
                inferredSchema={inferredSchema}
                incompatibleErrors={incompatibleSchemaErrors}
                key={testStreamIndex}
              />
            ),
            "data-testid": "tag-tab-detected-schema",
          },
        ]
      : []),
  ];

  // If the response is an error, default to the response tab
  const defaultTabIndex = page.response && page.response.status >= 400 ? 2 : 0;

  return <TabbedDisplay className={className} tabs={tabs} defaultTabIndex={defaultTabIndex} />;
};

const RecordDisplay = ({ records }: { records: StreamReadSlicesItemPagesItemRecordsItem[] }) => {
  const [recordViewMode, setRecordViewMode] = useLocalStorage("connectorBuilderRecordView", "json");
  const formattedRecords = useMemo(() => formatJson(records), [records]);

  return (
    <FlexContainer direction="column" className={styles.recordsContainer}>
      {records.length > 0 && (
        <FlexContainer justifyContent="flex-end" alignItems="center" gap="none">
          <Tooltip
            control={
              <Button
                variant="clear"
                type="button"
                className={recordViewMode === "table" ? styles.active : undefined}
                onClick={() => {
                  setRecordViewMode("table");
                }}
              >
                <Icon type="table" />
              </Button>
            }
          >
            <FormattedMessage id="connectorBuilder.tableViewMode" />
          </Tooltip>
          <Tooltip
            control={
              <Button
                variant="clear"
                type="button"
                className={recordViewMode === "json" ? styles.active : undefined}
                onClick={() => {
                  setRecordViewMode("json");
                }}
              >
                <Text size="sm" bold className={styles.jsonText}>
                  {"{ }"}
                </Text>
              </Button>
            }
          >
            <FormattedMessage id="connectorBuilder.jsonViewMode" />
          </Tooltip>
        </FlexContainer>
      )}
      <div className={styles.records}>
        {recordViewMode === "json" ? <Pre>{formattedRecords}</Pre> : <RecordTable records={records} />}
      </div>
    </FlexContainer>
  );
};
