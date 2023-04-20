import { useField } from "formik";
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
  BuilderIncrementalSync,
  DATETIME_FORMAT_OPTIONS,
  INCREMENTAL_SYNC_USER_INPUT_DATE_FORMAT,
  LARGE_DURATION_OPTIONS,
  SMALL_DURATION_OPTIONS,
} from "../types";

interface IncrementalSectionProps {
  streamFieldPath: (fieldPath: string) => string;
  currentStreamIndex: number;
}

const iso8601DurationAnchor = (
  <a href={links.iso8601Duration} target="_blank" rel="noreferrer">
    ISO 8601 duration
  </a>
);

export const IncrementalSection: React.FC<IncrementalSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();
  const [field, , helpers] = useField<BuilderIncrementalSync | undefined>(streamFieldPath("incrementalSync"));

  const handleToggle = (newToggleValue: boolean) => {
    if (newToggleValue) {
      helpers.setValue({
        datetime_format: "%Y-%m-%d %H:%M:%S.%f+00:00",
        start_datetime: { type: "user_input" },
        end_datetime: { type: "now" },
        step: "P1M",
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
      });
    } else {
      helpers.setValue(undefined);
    }
  };
  const toggledOn = field.value !== undefined;

  return (
    <BuilderCard
      toggleConfig={{
        label: (
          <ControlLabels
            label="Incremental sync"
            infoTooltipContent="Configure how to fetch data incrementally based on a time field in your data"
          />
        ),
        toggledOn,
        onToggle: handleToggle,
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
        label="Cursor field"
        tooltip="Field on record to use as the cursor"
      />
      <BuilderFieldWithInputs
        type="combobox"
        path={streamFieldPath("incrementalSync.datetime_format")}
        label="Datetime format"
        tooltip="Specify the format of the start and end time, e.g. %Y-%m-%d"
        options={DATETIME_FORMAT_OPTIONS}
      />
      <BuilderFieldWithInputs
        type="combobox"
        path={streamFieldPath("incrementalSync.cursor_granularity")}
        label="Cursor granularity"
        tooltip={
          <>
            Smallest increment the datetime format has ({iso8601DurationAnchor}) that will be used to ensure that the
            start of a slice does not overlap with the end of the previous one, e.g. for %Y-%m-%d the granularity should
            be P1D, for %Y-%m-%dT%H:%M:%SZ the granularity should be PT1S
          </>
        }
        options={SMALL_DURATION_OPTIONS}
      />
      <BuilderOneOf
        path={streamFieldPath("incrementalSync.start_datetime")}
        label="Start datetime"
        tooltip="Start time to start the sync from"
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
        label="End datetime"
        tooltip="Point in time to sync up to"
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
          label="Step"
          tooltip={
            <>
              Time interval ({iso8601DurationAnchor}) for which to break up stream into slices, e.g. P1D for daily
              slices
            </>
          }
          options={LARGE_DURATION_OPTIONS}
        />
        <BuilderFieldWithInputs
          type="combobox"
          path={streamFieldPath("incrementalSync.lookback_window")}
          label="Lookback window"
          tooltip={
            <>
              Time interval ({iso8601DurationAnchor}) before the start_datetime to read data for, e.g. P1M for looking
              back one month
            </>
          }
          options={LARGE_DURATION_OPTIONS}
        />
      </BuilderOptional>
    </BuilderCard>
  );
};
