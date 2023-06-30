import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import GroupControls from "components/GroupControls";
import { LabelInfo } from "components/Label";
import { ControlLabels } from "components/LabeledControl";

import { RequestOption } from "core/request/ConnectorManifest";
import { links } from "utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderOneOf } from "./BuilderOneOf";
import { RequestOptionFields } from "./RequestOptionFields";
import { ToggleGroupField } from "./ToggleGroupField";
import { CURSOR_PAGINATION, OFFSET_INCREMENT, PAGE_INCREMENT, StreamPathFn, useBuilderWatch } from "../types";

interface PaginationSectionProps {
  streamFieldPath: StreamPathFn;
  currentStreamIndex: number;
}

export const PaginationSection: React.FC<PaginationSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();
  const { setValue } = useFormContext();
  const pageSize = useBuilderWatch(streamFieldPath("paginator.strategy.page_size"));
  const pageSizeOptionPath = streamFieldPath("paginator.pageSizeOption");

  return (
    <BuilderCard
      docLink={links.connectorBuilderPagination}
      label="Pagination"
      tooltip="Configure how pagination is handled by your connector"
      toggleConfig={{
        path: streamFieldPath("paginator"),
        defaultValue: {
          strategy: {
            type: OFFSET_INCREMENT,
            page_size: "",
          },
          pageSizeOption: {
            inject_into: "request_parameter",
            field_name: "",
            type: "RequestOption",
          },
          pageTokenOption: {
            inject_into: "request_parameter",
            field_name: "",
          },
        },
      }}
      copyConfig={{
        path: "paginator",
        currentStreamIndex,
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromPaginationTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToPaginationTitle" }),
      }}
    >
      <BuilderOneOf
        path={streamFieldPath("paginator.strategy")}
        label="Mode"
        tooltip="Pagination method to use for requests sent to the API"
        manifestOptionPaths={["OffsetIncrement", "PageIncrement", "CursorPagination"]}
        options={[
          {
            label: "Offset Increment",
            typeValue: OFFSET_INCREMENT,
            default: {
              page_size: "",
            },
            children: (
              <>
                <BuilderField
                  type="number"
                  manifestPath="OffsetIncrement.properties.page_size"
                  path={streamFieldPath("paginator.strategy.page_size")}
                  optional
                />
                {pageSize ? <PageSizeOption label="limit" streamFieldPath={streamFieldPath} /> : null}
                <PageTokenOption label="offset" streamFieldPath={streamFieldPath} />
              </>
            ),
          },
          {
            label: "Page Increment",
            typeValue: PAGE_INCREMENT,
            default: {
              page_size: "",
              start_from_page: "",
            },
            children: (
              <>
                <BuilderField
                  type="number"
                  path={streamFieldPath("paginator.strategy.page_size")}
                  manifestPath="PageIncrement.properties.page_size"
                  optional
                />
                <BuilderField
                  type="number"
                  path={streamFieldPath("paginator.strategy.start_from_page")}
                  manifestPath="PageIncrement.properties.start_from_page"
                  optional
                />
                {pageSize ? <PageSizeOption label="page size" streamFieldPath={streamFieldPath} /> : null}
                <PageTokenOption label="page number" streamFieldPath={streamFieldPath} />
              </>
            ),
          },
          {
            label: "Cursor Pagination",
            typeValue: CURSOR_PAGINATION,
            default: {
              page_size: "",
              cursor: {
                type: "response",
                path: [],
              },
            },
            children: (
              <>
                <BuilderOneOf
                  path={streamFieldPath("paginator.strategy.cursor")}
                  label="Next page cursor"
                  tooltip={
                    <LabelInfo
                      description="Specify how to select the next page"
                      options={[
                        {
                          title: "Response",
                          description:
                            "Pointer to a field in the raw response body that will be used as the cursor token for fetching the next page of data",
                        },
                        {
                          title: "Header",
                          description:
                            "Pointer to a field in the raw response headers that will be used as the cursor token for fetching the next page of data",
                        },
                        {
                          title: "Custom",
                          description:
                            "Define a custom cursor token for fetching the next page of data which supports interpolated values, and a stop condition describing when to stop fetching more pages",
                        },
                      ]}
                    />
                  }
                  options={[
                    {
                      label: "Response",
                      typeValue: "response",
                      default: {
                        path: [],
                      },
                      children: (
                        <BuilderField
                          type="array"
                          path={streamFieldPath("paginator.strategy.cursor.path")}
                          label="Path"
                          tooltip="Path to the value in the response object to select"
                        />
                      ),
                    },
                    {
                      label: "Header",
                      typeValue: "headers",
                      default: {
                        path: [],
                      },
                      children: (
                        <BuilderField
                          type="array"
                          path={streamFieldPath("paginator.strategy.cursor.path")}
                          label="Path"
                          tooltip="Path to the header value to select"
                        />
                      ),
                    },
                    {
                      label: "Custom",
                      typeValue: "custom",
                      default: {
                        cursor_value: "",
                        stop_condition: "",
                      },
                      children: (
                        <>
                          <BuilderFieldWithInputs
                            type="string"
                            path={streamFieldPath("paginator.strategy.cursor.cursor_value")}
                            manifestPath="CursorPagination.properties.cursor_value"
                          />
                          <BuilderFieldWithInputs
                            type="string"
                            path={streamFieldPath("paginator.strategy.cursor.stop_condition")}
                            manifestPath="CursorPagination.properties.stop_condition"
                            optional
                          />
                        </>
                      ),
                    },
                  ]}
                />
                <PageTokenOption label="cursor value" streamFieldPath={streamFieldPath} />
                <BuilderField
                  type="number"
                  path={streamFieldPath("paginator.strategy.page_size")}
                  manifestPath="CursorPagination.properties.page_size"
                  onChange={(newValue) => {
                    if (newValue === undefined || newValue === "") {
                      setValue(pageSizeOptionPath, undefined);
                    }
                  }}
                  optional
                />
                {pageSize ? <PageSizeOption label="page size" streamFieldPath={streamFieldPath} /> : null}
              </>
            ),
          },
        ]}
      />
    </BuilderCard>
  );
};

const PageTokenOption = ({
  label,
  streamFieldPath,
}: {
  label: string;
  streamFieldPath: (fieldPath: string) => string;
}): JSX.Element => {
  return (
    <GroupControls
      label={
        <ControlLabels
          label={`Inject ${label} into outgoing HTTP request`}
          infoTooltipContent={`Configures how the ${label} will be sent in requests to the source API`}
        />
      }
    >
      <RequestOptionFields path={streamFieldPath("paginator.pageTokenOption")} descriptor={label} />
    </GroupControls>
  );
};

const PageSizeOption = ({
  label,
  streamFieldPath,
}: {
  label: string;
  streamFieldPath: (fieldPath: string) => string;
}): JSX.Element => {
  return (
    <ToggleGroupField<RequestOption>
      label={`Inject ${label} into outgoing HTTP request`}
      tooltip={`Configures how the ${label} will be sent in requests to the source API`}
      fieldPath={streamFieldPath("paginator.pageSizeOption")}
      initialValues={{
        inject_into: "request_parameter",
        type: "RequestOption",
        field_name: "",
      }}
    >
      <RequestOptionFields path={streamFieldPath("paginator.pageSizeOption")} descriptor={label} excludePathInjection />
    </ToggleGroupField>
  );
};
