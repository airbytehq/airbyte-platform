import { useField } from "formik";
import { useIntl } from "react-intl";

import { ControlLabels } from "components/LabeledControl";
import { FlexContainer } from "components/ui/Flex";

import { DefaultErrorHandlerBackoffStrategiesItem, HttpResponseFilter } from "core/request/ConnectorManifest";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderList } from "./BuilderList";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { getDescriptionByManifest, getOptionsByManifest } from "./manifestHelpers";
import { ToggleGroupField } from "./ToggleGroupField";
import { BuilderStream } from "../types";

interface PartitionSectionProps {
  streamFieldPath: (fieldPath: string) => string;
  currentStreamIndex: number;
}

export const ErrorHandlerSection: React.FC<PartitionSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();
  const [field, , helpers] = useField<BuilderStream["errorHandler"]>(streamFieldPath("errorHandler"));

  const handleToggle = (newToggleValue: boolean) => {
    if (newToggleValue) {
      helpers.setValue([
        {
          type: "DefaultErrorHandler",
        },
      ]);
    } else {
      helpers.setValue(undefined);
    }
  };
  const toggledOn = field.value !== undefined;

  const getBackoffOptions = (buildPath: (path: string) => string): OneOfOption[] => [
    {
      label: "Constant",
      typeValue: "ConstantBackoffStrategy",
      default: {
        backoff_time_in_seconds: 5,
      },
      children: (
        <BuilderField
          type="number"
          path={buildPath("backoff_strategy.backoff_time_in_seconds")}
          manifestPath="ConstantBackoffStrategy.properties.backoff_time_in_seconds"
        />
      ),
    },
    {
      label: "Exponential",
      typeValue: "ExponentialBackoffStrategy",
      default: {},
      children: (
        <BuilderField
          type="number"
          path={buildPath("backoff_strategy.factor")}
          manifestPath="ExponentialBackoffStrategy.properties.factor"
          optional
        />
      ),
    },
    {
      label: "Wait time from header",
      typeValue: "WaitTimeFromHeader",
      default: {},
      children: (
        <>
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.header")}
            manifestPath="WaitTimeFromHeader.properties.header"
          />
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.regex")}
            optional
            manifestPath="WaitTimeFromHeader.properties.regex"
          />
        </>
      ),
    },
    {
      label: "Wait until time from header",
      typeValue: "WaitUntilTimeFromHeader",
      default: {},
      children: (
        <>
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.header")}
            manifestPath="WaitUntilTimeFromHeader.properties.header"
          />
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.regex")}
            optional
            manifestPath="WaitUntilTimeFromHeader.properties.regex"
          />
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.min_wait")}
            optional
            manifestPath="WaitUntilTimeFromHeader.properties.min_wait"
          />
        </>
      ),
    },
  ];

  return (
    <BuilderCard
      toggleConfig={{
        label: (
          <ControlLabels label="Error handler" infoTooltipContent={getDescriptionByManifest("DefaultErrorHandler")} />
        ),
        toggledOn,
        onToggle: handleToggle,
      }}
      copyConfig={{
        path: "errorHandler",
        currentStreamIndex,
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromErrorHandlerTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToErrorHandlerTitle" }),
      }}
    >
      <BuilderList
        basePath={streamFieldPath("errorHandler")}
        emptyItem={{
          type: "DefaultErrorHandler",
          response_filters: [],
        }}
        addButtonLabel="Add error handler"
      >
        {({ buildPath }) => (
          <FlexContainer direction="column">
            <ToggleGroupField<DefaultErrorHandlerBackoffStrategiesItem>
              label="Backoff strategy"
              tooltip="Optionally configures how to retry a request multiple times"
              fieldPath={buildPath("backoff_strategy")}
              initialValues={{
                type: "ConstantBackoffStrategy",
                backoff_time_in_seconds: 5,
              }}
            >
              <BuilderOneOf
                path={buildPath("backoff_strategy")}
                label="Strategy"
                tooltip="The strategy to use to decide when to retry a request"
                manifestOptionPaths={[
                  "ConstantBackoffStrategy",
                  "ExponentialBackoffStrategy",
                  "WaitTimeFromHeader",
                  "WaitUntilTimeFromHeader",
                ]}
                options={getBackoffOptions(buildPath)}
              />
            </ToggleGroupField>
            <ToggleGroupField<HttpResponseFilter>
              label="Response filter"
              tooltip="Specify a filter to specify how to handle certain requests"
              fieldPath={buildPath("response_filter")}
              initialValues={{
                type: "HttpResponseFilter",
                action: "IGNORE",
              }}
            >
              <>
                <BuilderField
                  type="string"
                  path={buildPath("response_filter.error_message_contains")}
                  optional
                  manifestPath="HttpResponseFilter.properties.error_message_contains"
                />
                <BuilderField
                  type="string"
                  path={buildPath("response_filter.predicate")}
                  optional
                  pattern="{{ predicate logic }}"
                  manifestPath="HttpResponseFilter.properties.predicate"
                />
                <BuilderField
                  type="array"
                  path={buildPath("response_filter.http_codes")}
                  itemType="integer"
                  optional
                  manifestPath="HttpResponseFilter.properties.http_codes"
                />
                <BuilderField
                  type="enum"
                  path={buildPath("response_filter.action")}
                  options={getOptionsByManifest("HttpResponseFilter.properties.action")}
                  manifestPath="HttpResponseFilter.properties.action"
                />
                <BuilderField
                  type="string"
                  path={buildPath("response_filter.error_message")}
                  optional
                  manifestPath="HttpResponseFilter.properties.error_message"
                />
              </>
            </ToggleGroupField>
          </FlexContainer>
        )}
      </BuilderList>
    </BuilderCard>
  );
};
