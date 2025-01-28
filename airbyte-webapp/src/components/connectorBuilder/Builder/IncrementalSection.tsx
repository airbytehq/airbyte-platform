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
import { BuilderInputPlaceholder } from "./BuilderInputPlaceholder";
import { BuilderOneOf } from "./BuilderOneOf";
import { BuilderOptional } from "./BuilderOptional";
import { BuilderRequestInjection } from "./BuilderRequestInjection";
import { ToggleGroupField } from "./ToggleGroupField";
import { manifestIncrementalSyncToBuilder } from "../convertManifestToBuilderForm";
import {
  BuilderIncrementalSync,
  DATETIME_FORMAT_OPTIONS,
  INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
  LARGE_DURATION_OPTIONS,
  SMALL_DURATION_OPTIONS,
  StreamPathFn,
  builderIncrementalSyncToManifest,
  interpolateConfigKey,
  useBuilderWatch,
} from "../types";
import { LOCKED_INPUT_BY_INCREMENTAL_FIELD_NAME, useGetUniqueKey } from "../useLockedInputs";

interface IncrementalSectionProps {
  streamFieldPath: StreamPathFn;
  currentStreamIndex: number;
}

export const IncrementalSection: React.FC<IncrementalSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();
  const filterMode = useBuilderWatch(streamFieldPath("incrementalSync.filter_mode"));
  const getExistingOrUniqueKey = useGetUniqueKey();
  const label = formatMessage({ id: "connectorBuilder.incremental.label" });
  return (
    <BuilderCard
      docLink={links.connectorBuilderIncrementalSync}
      label={label}
      tooltip={formatMessage({ id: "connectorBuilder.incremental.tooltip" })}
      inputsConfig={{
        toggleable: true,
        path: streamFieldPath("incrementalSync"),
        defaultValue: {
          datetime_format: "",
          cursor_datetime_formats: [],
          start_datetime: {
            type: "user_input",
            value: interpolateConfigKey(
              getExistingOrUniqueKey(LOCKED_INPUT_BY_INCREMENTAL_FIELD_NAME.start_datetime.key, "start_datetime")
            ),
          },
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
        yamlConfig: {
          builderToManifest: builderIncrementalSyncToManifest,
          manifestToBuilder: manifestIncrementalSyncToBuilder,
        },
      }}
      copyConfig={{
        path: "incrementalSync",
        currentStreamIndex,
        componentName: label,
      }}
    >
      <CursorField streamFieldPath={streamFieldPath} />
      <CursorDatetimeFormatField streamFieldPath={streamFieldPath} />
      <BuilderField
        type="enum"
        options={[
          { value: "range", label: formatMessage({ id: "connectorBuilder.incremental.filterMode.range.label" }) },
          { value: "start", label: formatMessage({ id: "connectorBuilder.incremental.filterMode.start.label" }) },
          {
            value: "no_filter",
            label: formatMessage({ id: "connectorBuilder.incremental.filterMode.noFilter.label" }),
          },
        ]}
        path={streamFieldPath("incrementalSync.filter_mode")}
        label={formatMessage({ id: "connectorBuilder.incremental.filterMode.label" })}
        tooltip={
          <LabelInfo
            label=""
            description={formatMessage({ id: "connectorBuilder.incremental.filterMode.tooltip" })}
            options={[
              {
                title: formatMessage({ id: "connectorBuilder.incremental.filterMode.range.label" }),
                description: formatMessage({ id: "connectorBuilder.incremental.filterMode.range.tooltip" }),
              },
              {
                title: formatMessage({ id: "connectorBuilder.incremental.filterMode.start.label" }),
                description: formatMessage({ id: "connectorBuilder.incremental.filterMode.start.tooltip" }),
              },
              {
                title: formatMessage({ id: "connectorBuilder.incremental.filterMode.noFilter.label" }),
                description: formatMessage({ id: "connectorBuilder.incremental.filterMode.noFilter.tooltip" }),
              },
            ]}
          />
        }
      />
      {filterMode === "no_filter" && (
        <Message
          type="warning"
          text={formatMessage({ id: "connectorBuilder.incremental.filterMode.noFilter.warning" })}
        />
      )}
      <BuilderOneOf<BuilderIncrementalSync["start_datetime"]>
        path={streamFieldPath("incrementalSync.start_datetime")}
        label={
          filterMode === "no_filter"
            ? formatMessage({ id: "connectorBuilder.incremental.startDatetime.noFilterLabel" })
            : undefined
        }
        manifestPath="DatetimeBasedCursor.properties.start_datetime"
        options={[
          {
            label: formatMessage({ id: "connectorBuilder.incremental.userInput" }),
            default: {
              type: "user_input",
              value: interpolateConfigKey(
                getExistingOrUniqueKey(LOCKED_INPUT_BY_INCREMENTAL_FIELD_NAME.start_datetime.key, "start_datetime")
              ),
            },
            children: (
              <BuilderInputPlaceholder
                label={formatMessage({
                  id:
                    filterMode === "no_filter"
                      ? "connectorBuilder.incremental.userInput.startDatetime.label.noFilter"
                      : "connectorBuilder.incremental.userInput.startDatetime.label.default",
                })}
                tooltip={formatMessage({
                  id:
                    filterMode === "no_filter"
                      ? "connectorBuilder.incremental.userInput.startDatetime.tooltip.noFilter"
                      : "connectorBuilder.incremental.userInput.startDatetime.tooltip.default",
                })}
              />
            ),
          },
          {
            label: formatMessage({ id: "connectorBuilder.incremental.custom" }),
            default: {
              type: "custom",
              value: "",
              format: INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
            },
            children: (
              <>
                <BuilderField
                  type="jinja"
                  path={streamFieldPath("incrementalSync.start_datetime.value")}
                  label={formatMessage({ id: "connectorBuilder.incremental.custom.value.label" })}
                  tooltip={formatMessage({
                    id:
                      filterMode === "no_filter"
                        ? "connectorBuilder.incremental.custom.value.startDatetime.tooltip.noFilter"
                        : "connectorBuilder.incremental.custom.value.startDatetime.tooltip.default",
                  })}
                />
                <BuilderField
                  type="combobox"
                  options={DATETIME_FORMAT_OPTIONS}
                  path={streamFieldPath("incrementalSync.start_datetime.format")}
                  label={formatMessage({ id: "connectorBuilder.incremental.custom.format.label" })}
                  tooltip={formatMessage({
                    id:
                      filterMode === "no_filter"
                        ? "connectorBuilder.incremental.custom.format.startDatetime.tooltip.noFilter"
                        : "connectorBuilder.incremental.custom.format.startDatetime.tooltip.default",
                  })}
                  optional
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
              label: formatMessage({ id: "connectorBuilder.incremental.userInput" }),
              default: {
                type: "user_input",
                value: interpolateConfigKey(
                  getExistingOrUniqueKey(LOCKED_INPUT_BY_INCREMENTAL_FIELD_NAME.end_datetime.key, "end_datetime")
                ),
              },
              children: (
                <BuilderInputPlaceholder
                  label={formatMessage({ id: "connectorBuilder.incremental.userInput.endDatetime.label" })}
                  tooltip={formatMessage({ id: "connectorBuilder.incremental.userInput.endDatetime.tooltip" })}
                />
              ),
            },
            {
              label: formatMessage({ id: "connectorBuilder.incremental.now" }),
              default: { type: "now" },
            },
            {
              label: formatMessage({ id: "connectorBuilder.incremental.custom" }),
              default: {
                type: "custom",
                value: "",
                format: INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
              },
              children: (
                <>
                  <BuilderField
                    type="jinja"
                    path={streamFieldPath("incrementalSync.end_datetime.value")}
                    label={formatMessage({ id: "connectorBuilder.incremental.custom.value.label" })}
                    tooltip={formatMessage({ id: "connectorBuilder.incremental.custom.value.endDatetime.tooltip" })}
                  />
                  <BuilderField
                    type="combobox"
                    path={streamFieldPath("incrementalSync.end_datetime.format")}
                    label={formatMessage({ id: "connectorBuilder.incremental.custom.format.label" })}
                    tooltip={formatMessage({ id: "connectorBuilder.incremental.custom.format.endDatetime.tooltip" })}
                    options={DATETIME_FORMAT_OPTIONS}
                    optional
                  />
                </>
              ),
            },
          ]}
        />
      )}
      {filterMode !== "no_filter" && (
        <ToggleGroupField<RequestOption>
          label={formatMessage({ id: "connectorBuilder.incremental.startTimeInjectInto.label" })}
          tooltip={formatMessage({ id: "connectorBuilder.incremental.startTimeInjectInto.tooltip" })}
          fieldPath={streamFieldPath("incrementalSync.start_time_option")}
          initialValues={{
            inject_into: "request_parameter",
            type: "RequestOption",
            field_name: "",
          }}
        >
          <BuilderRequestInjection
            path={streamFieldPath("incrementalSync.start_time_option")}
            descriptor={formatMessage({ id: "connectorBuilder.incremental.startTimeInjectInto.descriptor" })}
            excludeValues={["path"]}
          />
        </ToggleGroupField>
      )}
      {filterMode === "range" && (
        <ToggleGroupField<RequestOption>
          label={formatMessage({ id: "connectorBuilder.incremental.endTimeInjectInto.label" })}
          tooltip={formatMessage({ id: "connectorBuilder.incremental.endTimeInjectInto.tooltip" })}
          fieldPath={streamFieldPath("incrementalSync.end_time_option")}
          initialValues={{
            inject_into: "request_parameter",
            type: "RequestOption",
            field_name: "",
          }}
        >
          <BuilderRequestInjection
            path={streamFieldPath("incrementalSync.end_time_option")}
            descriptor={formatMessage({ id: "connectorBuilder.incremental.endTimeInjectInto.descriptor" })}
            excludeValues={["path"]}
          />
        </ToggleGroupField>
      )}
      <BuilderField
        type="combobox"
        path={streamFieldPath("incrementalSync.datetime_format")}
        manifestPath="DatetimeBasedCursor.properties.datetime_format"
        options={DATETIME_FORMAT_OPTIONS}
        optional
        label={formatMessage({ id: "connectorBuilder.incremental.outgoingDatetimeFormat.label" })}
        tooltip={
          <>
            {formatMessage({ id: "connectorBuilder.incremental.outgoingDatetimeFormat.tooltip" })}
            <ReactMarkdown>{formatMessage({ id: "connectorBuilder.incremental.formatPlaceholders" })}</ReactMarkdown>
          </>
        }
      />
      <BuilderOptional label={formatMessage({ id: "connectorBuilder.advancedFields" })}>
        {filterMode !== "no_filter" && (
          <ToggleGroupField<BuilderIncrementalSync["slicer"]>
            label={formatMessage({ id: "connectorBuilder.incremental.splitUpInterval.label" })}
            tooltip={formatMessage({ id: "connectorBuilder.incremental.splitUpInterval.tooltip" })}
            fieldPath={streamFieldPath("incrementalSync.slicer")}
            initialValues={{
              step: "",
              cursor_granularity: "",
            }}
          >
            <BuilderField
              type="combobox"
              path={streamFieldPath("incrementalSync.slicer.step")}
              manifestPath="DatetimeBasedCursor.properties.step"
              options={LARGE_DURATION_OPTIONS}
            />
            <BuilderField
              type="combobox"
              path={streamFieldPath("incrementalSync.slicer.cursor_granularity")}
              manifestPath="DatetimeBasedCursor.properties.cursor_granularity"
              options={SMALL_DURATION_OPTIONS}
            />
          </ToggleGroupField>
        )}
        <BuilderField
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
    <BuilderField
      preview={(fieldValue) => {
        const mostRecentRecordValues = data?.slices?.at(0)?.pages.at(0)?.records.at(0);
        const cursorValue = mostRecentRecordValues?.[fieldValue];
        return cursorValue != null ? (
          <FormattedMessage id="connectorBuilder.incremental.cursorValuePreview" values={{ cursorValue }} />
        ) : undefined;
      }}
      type={datetimeFields.length > 0 ? "combobox" : "jinja"}
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
      {!cursorDatetimeFormats?.includes(detectedFormat) && cursorField && detectedFormat && (
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
        label={formatMessage({ id: "connectorBuilder.incremental.cursorDatetimeFormat.label" })}
        tooltip={
          <>
            {formatMessage({ id: "connectorBuilder.incremental.cursorDatetimeFormat.tooltip" })}
            <ReactMarkdown>{formatMessage({ id: "connectorBuilder.incremental.formatPlaceholders" })}</ReactMarkdown>
          </>
        }
      />
    </>
  );
};
