import { faTable } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { Tab } from "@headlessui/react";
import classNames from "classnames";
import React, { ReactNode, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocalStorage } from "react-use";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Pre } from "components/ui/Pre";
import { Text } from "components/ui/Text";
import { InfoTooltip, Tooltip } from "components/ui/Tooltip";

import {
  HttpRequest,
  HttpResponse,
  StreamReadInferredSchema,
  StreamReadSlicesItemPagesItem,
} from "core/api/types/ConnectorBuilderClient";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./PageDisplay.module.scss";
import { RecordTable } from "./RecordTable";
import { SchemaDiffView } from "./SchemaDiffView";
import { SchemaConflictIndicator } from "../SchemaConflictIndicator";
import { formatJson } from "../utils";

interface PageDisplayProps {
  page: StreamReadSlicesItemPagesItem;
  inferredSchema?: StreamReadInferredSchema;
  className?: string;
}

interface TabData {
  title: ReactNode;
  key: string;
  content: string;
}

function formatForDisplay(obj: HttpRequest | HttpResponse | undefined): string {
  if (!obj) {
    return "";
  }

  let parsedBody: unknown;
  try {
    // body is a string containing JSON most of the time, but not always.
    // Attempt to parse and fall back to the raw string if unsuccessful.
    parsedBody = obj.body ? JSON.parse(obj.body) : "";
  } catch {
    parsedBody = obj.body;
  }

  const unpackedObj = {
    ...obj,
    body: parsedBody,
  };
  return formatJson(unpackedObj);
}

export const PageDisplay: React.FC<PageDisplayProps> = ({ page, className, inferredSchema }) => {
  const { formatMessage } = useIntl();

  const {
    editorView,
    builderFormValues: { streams: builderFormStreams },
  } = useConnectorBuilderFormState();
  const {
    streamRead,
    schemaWarnings: { incompatibleSchemaErrors, schemaDifferences },
    testStreamIndex,
  } = useConnectorBuilderTestRead();

  const [recordViewMode, setRecordViewMode] = useLocalStorage<"json" | "table">("connectorBuilderRecordView", "json");

  const autoImportSchema = builderFormStreams[testStreamIndex]?.autoImportSchema;

  const formattedRecords = useMemo(() => formatJson(page.records), [page.records]);
  const formattedRequest = useMemo(() => formatForDisplay(page.request), [page.request]);
  const formattedResponse = useMemo(() => formatForDisplay(page.response), [page.response]);

  let defaultTabIndex = 0;
  const tabs: TabData[] = [];
  if (page.request) {
    tabs.push({
      title: formatMessage({ id: "connectorBuilder.requestTab" }),
      key: "request",
      content: formattedRequest,
    });
  }
  if (page.response) {
    tabs.push({
      title: formatMessage({ id: "connectorBuilder.responseTab" }),
      key: "response",
      content: formattedResponse,
    });

    if (page.response.status >= 400) {
      // If the response is an error, default to the response tab
      defaultTabIndex = tabs.length;
    }
  }

  const [selectedIndex, setSelectedIndex] = useState(defaultTabIndex);

  return (
    <div className={classNames(className)}>
      <Tab.Group selectedIndex={selectedIndex} onChange={setSelectedIndex}>
        <Tab.List className={styles.tabList}>
          <Tab className={styles.tab}>
            {({ selected }) => (
              <Text className={classNames(styles.tabTitle, { [styles.selected]: selected })} size="xs" align="center">
                {`${formatMessage({ id: "connectorBuilder.recordsTab" })} (${page.records.length})`}
                {!streamRead.isFetching && streamRead.data && streamRead.data.test_read_limit_reached && (
                  <InfoTooltip>
                    <FormattedMessage id="connectorBuilder.streamTestLimitReached" />
                  </InfoTooltip>
                )}
              </Text>
            )}
          </Tab>
          {tabs.map((tab) => (
            <Tab className={styles.tab} key={tab.key}>
              {({ selected }) => (
                <Text className={classNames(styles.tabTitle, { [styles.selected]: selected })} size="xs" align="center">
                  {tab.title}
                </Text>
              )}
            </Tab>
          ))}
          {inferredSchema && (
            <Tab className={styles.tab} data-testid="tag-tab-detected-schema">
              {({ selected }) => (
                <Text className={classNames(styles.tabTitle, { [styles.selected]: selected })} as="div" size="xs">
                  <FlexContainer direction="row" justifyContent="center">
                    {formatMessage({ id: "connectorBuilder.schemaTab" })}
                    {editorView === "ui" && schemaDifferences && !autoImportSchema && (
                      <SchemaConflictIndicator errors={incompatibleSchemaErrors} />
                    )}
                  </FlexContainer>
                </Text>
              )}
            </Tab>
          )}
        </Tab.List>
        {page.records.length > 0 && selectedIndex === 0 && (
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
                  <FontAwesomeIcon icon={faTable} />
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
        <Tab.Panels className={styles.tabPanelContainer}>
          <Tab.Panel className={styles.tabPanel}>
            {recordViewMode === "json" ? <Pre>{formattedRecords}</Pre> : <RecordTable records={page.records} />}
          </Tab.Panel>
          {tabs.map((tab) => (
            <Tab.Panel className={styles.tabPanel} key={tab.key}>
              <Pre>{tab.content}</Pre>
            </Tab.Panel>
          ))}
          {inferredSchema && (
            <Tab.Panel className={styles.tabPanel}>
              <SchemaDiffView
                inferredSchema={inferredSchema}
                incompatibleErrors={incompatibleSchemaErrors}
                key={testStreamIndex}
              />
            </Tab.Panel>
          )}
        </Tab.Panels>
      </Tab.Group>
    </div>
  );
};
