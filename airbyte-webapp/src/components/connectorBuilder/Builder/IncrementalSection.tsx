import React from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { ReactMarkdown } from "react-markdown/lib/react-markdown";

import { LabelInfo } from "components/Label";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { RequestOption } from "core/api/types/ConnectorManifest";
import { links } from "core/utils/links";
import { useConnectorBuilderTestRead } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderInputPlaceholder } from "./BuilderInputPlaceholder";
import { BuilderOneOf } from "./BuilderOneOf";
import { BuilderOptional } from "./BuilderOptional";
import { InjectIntoFields } from "./InjectIntoFields";
import { ToggleGroupField } from "./ToggleGroupField";
import {
  BuilderIncrementalSync,
  DATETIME_FORMAT_OPTIONS,
  INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
  LARGE_DURATION_OPTIONS,
  SMALL_DURATION_OPTIONS,
  StreamPathFn,
  useBuilderWatch,
} from "../types";

interface IncrementalSectionProps {
  streamFieldPath: StreamPathFn;
  currentStreamIndex: number;
}

export const IncrementalSection: React.FC<IncrementalSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();
  const filterMode = useBuilderWatch(streamFieldPath("incrementalSync.filter_mode"));
  return (
    <BuilderCard
      docLink={links.connectorBuilderIncrementalSync}
      label="Incremental Sync"
      tooltip="Configure how to fetch data incrementally based on a time field in your data"
      toggleConfig={{
        path: streamFieldPath("incrementalSync"),
        defaultValue: {
          datetime_format: "",
          cursor_datetime_formats: [],
          start_datetime: { type: "user_input" },
          end_datetime: { type: "now" },
          step: "",
          cursor_field: "",
          cursor_granularity: "",
          filter_mode: "range",
          start_time_option: {
            inject_into: "request_parameter",
            field_name: "",
            type: "RequestOption",
          },
          end_time_option: {
            inject_into: "request_parameter",
            field_name: "",
            type: "RequestOption",
          },
        },
      }}
      copyConfig={{
        path: "incrementalSync",
        currentStreamIndex,
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromIncrementalTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToIncrementalTitle" }),
      }}
    >
      <CursorField streamFieldPath={streamFieldPath} />
      <CursorDatetimeFormatField streamFieldPath={streamFieldPath} />
      <BuilderField
        type="enum"
        options={[
          { value: "range", label: "Range" },
          { value: "start", label: "Start" },
          { value: "no_filter", label: "No filter (data feed)" },
        ]}
        path={streamFieldPath("incrementalSync.filter_mode")}
        label="API Time Filtering Capabilities"
        tooltip={
          <LabelInfo
            label=""
            description="The capabilities of the API to filter data based on the cursor field."
            options={[
              {
                title: "Range",
                description: "The API can filter data based on a range (start and end datetime) of the cursor field.",
              },
              {
                title: "Start",
                description:
                  "The API can filter data based on the start datetime, but will always include all data from the start date up to now",
              },
              {
                title: "No filter (data feed)",
                description:
                  "The API can't filter by the cursor field. When choosing this option, make sure the data is ordered descending on the cursor field (newest to oldest)",
              },
            ]}
          />
        }
      />
      {filterMode === "no_filter" && (
        <Message
          type="warning"
          text="The data must be returned in descending order for the cursor field across pages in order for incremental sync to work properly with no time filtering. If the data is returned in ascending order, you should not configure incremental syncs for this API."
        />
      )}
      <BuilderOneOf<BuilderIncrementalSync["start_datetime"]>
        path={streamFieldPath("incrementalSync.start_datetime")}
        label={filterMode === "no_filter" ? "Earliest datetime cutoff" : undefined}
        manifestPath="DatetimeBasedCursor.properties.start_datetime"
        options={[
          {
            label: "User input",
            default: { type: "user_input" },
            children: (
              <BuilderInputPlaceholder
                label="Start date user input"
                tooltip={
                  filterMode === "no_filter"
                    ? "The cutoff time as a user input. Fill it in in the testing values"
                    : "The time to start syncing as a user input. Fill it in in the testing values"
                }
              />
            ),
          },
          {
            label: "Custom",
            default: {
              type: "custom",
              value: "",
              format: INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
            },
            children: (
              <>
                <BuilderFieldWithInputs
                  type="string"
                  path={streamFieldPath("incrementalSync.start_datetime.value")}
                  label="Value"
                  tooltip="The time to start syncing"
                />
                <BuilderFieldWithInputs
                  type="combobox"
                  options={DATETIME_FORMAT_OPTIONS}
                  path={streamFieldPath("incrementalSync.start_datetime.format")}
                  label="Format"
                  optional
                  tooltip="The format of the provided start date. If not specified, the format of the format of the cursor value is used"
                />
              </>
            ),
          },
        ]}
      />
      {filterMode === "range" && (
        <BuilderOneOf<BuilderIncrementalSync["end_datetime"]>
          path={streamFieldPath("incrementalSync.end_datetime")}
          manifestPath="DatetimeBasedCursor.properties.end_datetime"
          options={[
            {
              label: "User input",
              default: { type: "user_input" },
              children: (
                <BuilderInputPlaceholder
                  label="End date user input"
                  tooltip="The time up to which to sync a user input. Fill it in in the testing values"
                />
              ),
            },
            {
              label: "Now",
              default: { type: "now" },
            },
            {
              label: "Custom",
              default: {
                type: "custom",
                value: "",
                format: INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
              },
              children: (
                <>
                  <BuilderFieldWithInputs
                    type="string"
                    path={streamFieldPath("incrementalSync.end_datetime.value")}
                    label="Value"
                    tooltip="The time up to which to sync"
                  />
                  <BuilderFieldWithInputs
                    type="combobox"
                    path={streamFieldPath("incrementalSync.end_datetime.format")}
                    label="Format"
                    options={DATETIME_FORMAT_OPTIONS}
                    optional
                    tooltip="The format of the provided end date. If not specified, the format of the cursor value is used"
                  />
                </>
              ),
            },
          ]}
        />
      )}
      {filterMode !== "no_filter" && (
        <ToggleGroupField<RequestOption>
          label="Inject Start Time into outgoing HTTP Request"
          tooltip="Optionally configures how the start datetime will be sent in requests to the source API"
          fieldPath={streamFieldPath("incrementalSync.start_time_option")}
          initialValues={{
            inject_into: "request_parameter",
            type: "RequestOption",
            field_name: "",
          }}
        >
          <InjectIntoFields
            path={streamFieldPath("incrementalSync.start_time_option")}
            descriptor="start datetime"
            excludeValues={["path"]}
          />
        </ToggleGroupField>
      )}
      {filterMode === "range" && (
        <ToggleGroupField<RequestOption>
          label="Inject End Time into outgoing HTTP Request"
          tooltip="Optionally configures how the end datetime will be sent in requests to the source API"
          fieldPath={streamFieldPath("incrementalSync.end_time_option")}
          initialValues={{
            inject_into: "request_parameter",
            type: "RequestOption",
            field_name: "",
          }}
        >
          <InjectIntoFields
            path={streamFieldPath("incrementalSync.end_time_option")}
            descriptor="end datetime"
            excludeValues={["path"]}
          />
        </ToggleGroupField>
      )}
      <BuilderOptional label={formatMessage({ id: "connectorBuilder.advancedFields" })}>
        <BuilderFieldWithInputs
          type="combobox"
          path={streamFieldPath("incrementalSync.datetime_format")}
          manifestPath="DatetimeBasedCursor.properties.datetime_format"
          options={DATETIME_FORMAT_OPTIONS}
          optional
          tooltip={
            <>
              The datetime format used to format the datetime values that are sent in outgoing requests to the API. If
              not provided, the first format in Cursor Datetime Formats will be used.
              <ReactMarkdown>{formatMessage({ id: "connectorBuilder.incremental.formatPlaceholders" })}</ReactMarkdown>
            </>
          }
        />
        {filterMode !== "no_filter" && (
          <ToggleGroupField<BuilderIncrementalSync["slicer"]>
            label="Split Up Interval"
            tooltip="Optionally split up the interval into smaller chunks to reduce the amount of data fetched in a single request and to make the sync more resilient to failures"
            fieldPath={streamFieldPath("incrementalSync.slicer")}
            initialValues={{
              step: "",
              cursor_granularity: "",
            }}
          >
            <BuilderFieldWithInputs
              type="combobox"
              path={streamFieldPath("incrementalSync.slicer.step")}
              manifestPath="DatetimeBasedCursor.properties.step"
              options={LARGE_DURATION_OPTIONS}
            />
            <BuilderFieldWithInputs
              type="combobox"
              path={streamFieldPath("incrementalSync.slicer.cursor_granularity")}
              manifestPath="DatetimeBasedCursor.properties.cursor_granularity"
              options={SMALL_DURATION_OPTIONS}
            />
          </ToggleGroupField>
        )}
        <BuilderFieldWithInputs
          type="combobox"
          path={streamFieldPath("incrementalSync.lookback_window")}
          manifestPath="DatetimeBasedCursor.properties.lookback_window"
          options={LARGE_DURATION_OPTIONS}
          optional
        />
      </BuilderOptional>
    </BuilderCard>
  );
};

const CURSOR_PATH = "incrementalSync.cursor_field";
const CURSOR_DATETIME_FORMATS_PATH = "incrementalSync.cursor_datetime_formats";

const CursorField = ({ streamFieldPath }: { streamFieldPath: StreamPathFn }) => {
  const {
    streamRead: { data },
  } = useConnectorBuilderTestRead();

  const datetimeFields = Object.keys(data?.inferred_datetime_formats || {});

  return (
    <BuilderFieldWithInputs
      type={datetimeFields.length > 0 ? "combobox" : "string"}
      path={streamFieldPath(CURSOR_PATH)}
      manifestPath="DatetimeBasedCursor.properties.cursor_field"
      options={datetimeFields.map((field) => ({ label: field, value: field }))}
    />
  );
};

const CursorDatetimeFormatField = ({ streamFieldPath }: { streamFieldPath: StreamPathFn }) => {
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();
  const cursorDatetimeFormats = useBuilderWatch(streamFieldPath(CURSOR_DATETIME_FORMATS_PATH));
  const cursorField = useBuilderWatch(streamFieldPath(CURSOR_PATH));
  const {
    streamRead: { data },
  } = useConnectorBuilderTestRead();
  const detectedFormat = data?.inferred_datetime_formats?.[cursorField];
  return (
    <>
      {!cursorDatetimeFormats.includes(detectedFormat) && cursorField && detectedFormat && (
        <Message
          type="info"
          text={
            <FormattedMessage
              id="connectorBuilder.matchingFormat"
              values={{
                format: (
                  <Text as="span" bold>
                    {detectedFormat}
                  </Text>
                ),
              }}
            />
          }
          actionBtnText={<FormattedMessage id="form.apply" />}
          onAction={() => {
            setValue(
              streamFieldPath(CURSOR_DATETIME_FORMATS_PATH),
              [detectedFormat, ...(cursorDatetimeFormats ? cursorDatetimeFormats : [])],
              {
                shouldValidate: true,
              }
            );
          }}
        />
      )}
      <BuilderField
        type="multicombobox"
        path={streamFieldPath(CURSOR_DATETIME_FORMATS_PATH)}
        // explicitly using a different manifest path here in order to pull in the examples from the manifest
        manifestPath="DatetimeBasedCursor.properties.datetime_format"
        options={DATETIME_FORMAT_OPTIONS}
        label="Cursor Datetime Formats"
        tooltip={
          <>
            The possible formats for the cursor field, in order of preference. The first format that matches the cursor
            field value will be used to parse it.
            <ReactMarkdown>{formatMessage({ id: "connectorBuilder.incremental.formatPlaceholders" })}</ReactMarkdown>
          </>
        }
      />
    </>
  );
};
