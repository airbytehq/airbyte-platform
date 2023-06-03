import React from "react";
import { useIntl } from "react-intl";

import { ControlLabels } from "components/LabeledControl";

import { RequestOption } from "core/request/ConnectorManifest";
import { links } from "utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderInputPlaceholder } from "./BuilderInputPlaceholder";
import { BuilderOneOf } from "./BuilderOneOf";
import { BuilderOptional } from "./BuilderOptional";
import { RequestOptionFields } from "./RequestOptionFields";
import { ToggleGroupField } from "./ToggleGroupField";
import {
  DATETIME_FORMAT_OPTIONS,
  INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
  LARGE_DURATION_OPTIONS,
  SMALL_DURATION_OPTIONS,
  StreamPathFn,
} from "../types";

interface IncrementalSectionProps {
  streamFieldPath: StreamPathFn;
  currentStreamIndex: number;
}

export const IncrementalSection: React.FC<IncrementalSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();

  return (
    <BuilderCard
      docLink={links.connectorBuilderIncrementalSync}
      label={
        <ControlLabels
          label="Incremental Sync"
          infoTooltipContent="Configure how to fetch data incrementally based on a time field in your data"
        />
      }
      toggleConfig={{
        path: streamFieldPath("incrementalSync"),
        defaultValue: {
          datetime_format: "%Y-%m-%d %H:%M:%S.%f+00:00",
          start_datetime: { type: "user_input" },
          end_datetime: { type: "now" },
          step: "",
          cursor_field: "",
          cursor_granularity: "",
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
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyToIncrementalTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyFromIncrementalTitle" }),
      }}
    >
      <BuilderFieldWithInputs
        type="string"
        path={streamFieldPath("incrementalSync.cursor_field")}
        manifestPath="DatetimeBasedCursor.properties.cursor_field"
      />
      <BuilderFieldWithInputs
        type="combobox"
        path={streamFieldPath("incrementalSync.datetime_format")}
        manifestPath="DatetimeBasedCursor.properties.datetime_format"
        options={DATETIME_FORMAT_OPTIONS}
      />
      <BuilderFieldWithInputs
        type="combobox"
        path={streamFieldPath("incrementalSync.cursor_granularity")}
        manifestPath="DatetimeBasedCursor.properties.cursor_granularity"
        options={SMALL_DURATION_OPTIONS}
      />
      <BuilderOneOf
        path={streamFieldPath("incrementalSync.start_datetime")}
        manifestPath="DatetimeBasedCursor.properties.start_datetime"
        options={[
          {
            label: "User input",
            typeValue: "user_input",
            default: {},
            children: (
              <BuilderInputPlaceholder
                label="Start date user input"
                tooltip="The time to start syncing as a user input. Fill it in in the testing values"
              />
            ),
          },
          {
            label: "Custom",
            typeValue: "custom",
            default: {
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
      <BuilderOneOf
        path={streamFieldPath("incrementalSync.end_datetime")}
        manifestPath="DatetimeBasedCursor.properties.end_datetime"
        options={[
          {
            label: "User input",
            typeValue: "user_input",
            default: {},
            children: (
              <BuilderInputPlaceholder
                label="End date user input"
                tooltip="The time up to which to sync a user input. Fill it in in the testing values"
              />
            ),
          },
          {
            label: "Now",
            typeValue: "now",
            default: {},
          },
          {
            label: "Custom",
            typeValue: "custom",
            default: {
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
                  tooltip="The format of the provided end date. If not specified, the format of the format of the cursor value is used"
                />
              </>
            ),
          },
        ]}
      />
      <ToggleGroupField<RequestOption>
        label="Inject start time into outgoing HTTP request"
        tooltip="Optionally configures how the start datetime will be sent in requests to the source API"
        fieldPath={streamFieldPath("incrementalSync.start_time_option")}
        initialValues={{
          inject_into: "request_parameter",
          type: "RequestOption",
          field_name: "",
        }}
      >
        <RequestOptionFields
          path={streamFieldPath("incrementalSync.start_time_option")}
          descriptor="start datetime"
          excludePathInjection
        />
      </ToggleGroupField>
      <ToggleGroupField<RequestOption>
        label="Inject end time into outgoing HTTP request"
        tooltip="Optionally configures how the end datetime will be sent in requests to the source API"
        fieldPath={streamFieldPath("incrementalSync.end_time_option")}
        initialValues={{
          inject_into: "request_parameter",
          type: "RequestOption",
          field_name: "",
        }}
      >
        <RequestOptionFields
          path={streamFieldPath("incrementalSync.end_time_option")}
          descriptor="end datetime"
          excludePathInjection
        />
      </ToggleGroupField>
      <BuilderOptional label={formatMessage({ id: "connectorBuilder.advancedFields" })}>
        <BuilderFieldWithInputs
          type="combobox"
          path={streamFieldPath("incrementalSync.step")}
          manifestPath="DatetimeBasedCursor.properties.step"
          options={LARGE_DURATION_OPTIONS}
          optional
        />
        <BuilderFieldWithInputs
          type="combobox"
          path={streamFieldPath("incrementalSync.lookback_window")}
          manifestPath="DatetimeBasedCursor.properties.lookback_window"
          options={LARGE_DURATION_OPTIONS}
        />
      </BuilderOptional>
    </BuilderCard>
  );
};
