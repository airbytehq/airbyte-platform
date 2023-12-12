import lowerCase from "lodash/lowerCase";
import { useFormContext } from "react-hook-form";
import { useIntl } from "react-intl";

import { LabelInfo } from "components/Label";

import { RequestOption } from "core/api/types/ConnectorManifest";
import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderFieldWithInputs } from "./BuilderFieldWithInputs";
import { BuilderOneOf } from "./BuilderOneOf";
import { BuilderRequestInjection } from "./BuilderRequestInjection";
import { ToggleGroupField } from "./ToggleGroupField";
import {
  BuilderCursorPagination,
  BuilderPaginator,
  CURSOR_PAGINATION,
  OFFSET_INCREMENT,
  PAGE_INCREMENT,
  StreamPathFn,
  useBuilderWatch,
} from "../types";

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
      label={formatMessage({ id: "connectorBuilder.pagination.label" })}
      tooltip={formatMessage({ id: "connectorBuilder.pagination.tooltip" })}
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
      <BuilderOneOf<BuilderPaginator["strategy"]>
        path={streamFieldPath("paginator.strategy")}
        label={formatMessage({ id: "connectorBuilder.pagination.strategy.label" })}
        tooltip={formatMessage({ id: "connectorBuilder.pagination.strategy.tooltip" })}
        manifestOptionPaths={[OFFSET_INCREMENT, PAGE_INCREMENT, CURSOR_PAGINATION]}
        options={[
          {
            label: formatMessage({ id: "connectorBuilder.pagination.strategy.offsetIncrement" }),
            default: {
              type: OFFSET_INCREMENT,
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
                {pageSize ? (
                  <PageSizeOption
                    label={formatMessage({ id: "connectorBuilder.pagination.strategy.offsetIncrement.limit" })}
                    streamFieldPath={streamFieldPath}
                  />
                ) : null}
                <PageTokenOption
                  label={formatMessage({ id: "connectorBuilder.pagination.strategy.offsetIncrement.offset" })}
                  streamFieldPath={streamFieldPath}
                  paginatorStrategy={OFFSET_INCREMENT}
                />
              </>
            ),
          },
          {
            label: formatMessage({ id: "connectorBuilder.pagination.strategy.pageIncrement" }),
            default: {
              type: PAGE_INCREMENT,
              page_size: undefined,
              start_from_page: undefined,
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
                {pageSize ? (
                  <PageSizeOption
                    label={formatMessage({ id: "connectorBuilder.pagination.strategy.pageSize" })}
                    streamFieldPath={streamFieldPath}
                  />
                ) : null}
                <PageTokenOption
                  label={formatMessage({ id: "connectorBuilder.pagination.strategy.pageIncrement.pageNumber" })}
                  streamFieldPath={streamFieldPath}
                  paginatorStrategy={PAGE_INCREMENT}
                />
              </>
            ),
          },
          {
            label: formatMessage({ id: "connectorBuilder.pagination.strategy.cursor" }),
            default: {
              type: CURSOR_PAGINATION,
              page_size: undefined,
              cursor: {
                type: "response",
                path: [],
              },
            },
            children: (
              <>
                <BuilderOneOf<BuilderCursorPagination["cursor"]>
                  path={streamFieldPath("paginator.strategy.cursor")}
                  label={formatMessage({ id: "connectorBuilder.pagination.strategy.cursor.cursor.label" })}
                  tooltip={
                    <LabelInfo
                      description={formatMessage({ id: "connectorBuilder.pagination.strategy.cursor.cursor.tooltip" })}
                      options={[
                        {
                          title: formatMessage({
                            id: "connectorBuilder.pagination.strategy.cursor.cursor.response.label",
                          }),
                          description: formatMessage({
                            id: "connectorBuilder.pagination.strategy.cursor.cursor.response.tooltip",
                          }),
                        },
                        {
                          title: formatMessage({
                            id: "connectorBuilder.pagination.strategy.cursor.cursor.header.label",
                          }),
                          description: formatMessage({
                            id: "connectorBuilder.pagination.strategy.cursor.cursor.header.tooltip",
                          }),
                        },
                        {
                          title: formatMessage({
                            id: "connectorBuilder.pagination.strategy.cursor.cursor.custom.label",
                          }),
                          description: formatMessage({
                            id: "connectorBuilder.pagination.strategy.cursor.cursor.custom.tooltip",
                          }),
                        },
                      ]}
                    />
                  }
                  options={[
                    {
                      label: formatMessage({ id: "connectorBuilder.pagination.strategy.cursor.cursor.response.label" }),
                      default: {
                        type: "response",
                        path: [],
                      },
                      children: (
                        <BuilderField
                          type="array"
                          path={streamFieldPath("paginator.strategy.cursor.path")}
                          label={formatMessage({ id: "connectorBuilder.pagination.strategy.cursor.cursor.path" })}
                          tooltip={formatMessage({
                            id: "connectorBuilder.pagination.strategy.cursor.cursor.response.path",
                          })}
                        />
                      ),
                    },
                    {
                      label: "Header",
                      default: {
                        type: "headers",
                        path: [],
                      },
                      children: (
                        <BuilderField
                          type="array"
                          path={streamFieldPath("paginator.strategy.cursor.path")}
                          label={formatMessage({ id: "connectorBuilder.pagination.strategy.cursor.cursor.path" })}
                          tooltip={formatMessage({
                            id: "connectorBuilder.pagination.strategy.cursor.cursor.header.path",
                          })}
                        />
                      ),
                    },
                    {
                      label: formatMessage({ id: "connectorBuilder.pagination.strategy.cursor.cursor.custom.label" }),
                      default: {
                        type: "custom",
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
                <PageTokenOption
                  label={formatMessage({ id: "connectorBuilder.pagination.strategy.cursorValue" })}
                  streamFieldPath={streamFieldPath}
                  paginatorStrategy={CURSOR_PAGINATION}
                />
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
                {pageSize ? (
                  <PageSizeOption
                    label={formatMessage({ id: "connectorBuilder.pagination.strategy.pageSize" })}
                    streamFieldPath={streamFieldPath}
                  />
                ) : null}
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
  paginatorStrategy,
}: {
  label: string;
  streamFieldPath: (fieldPath: string) => string;
  paginatorStrategy: typeof OFFSET_INCREMENT | typeof PAGE_INCREMENT | typeof CURSOR_PAGINATION;
}): JSX.Element => {
  const { formatMessage } = useIntl();

  return (
    <ToggleGroupField<RequestOption>
      label={formatMessage({ id: "connectorBuilder.injection.label" }, { label })}
      tooltip={formatMessage({ id: "connectorBuilder.injection.tooltip" }, { label: lowerCase(label) })}
      fieldPath={streamFieldPath("paginator.pageTokenOption")}
      initialValues={{
        inject_into: "request_parameter",
        type: "RequestOption",
        field_name: "",
      }}
    >
      <BuilderRequestInjection path={streamFieldPath("paginator.pageTokenOption")} descriptor={lowerCase(label)} />
      {paginatorStrategy !== CURSOR_PAGINATION && (
        <BuilderField
          type="boolean"
          path={streamFieldPath("paginator.strategy.inject_on_first_request")}
          label={formatMessage({ id: "connectorBuilder.pagination.strategy.injectOnFirstRequest.label" })}
          tooltip={formatMessage({
            id:
              paginatorStrategy === OFFSET_INCREMENT
                ? "connectorBuilder.pagination.strategy.injectOnFirstRequest.tooltip.offsetIncrement"
                : "connectorBuilder.pagination.strategy.injectOnFirstRequest.tooltip.pageIncrement",
          })}
        />
      )}
    </ToggleGroupField>
  );
};

const PageSizeOption = ({
  label,
  streamFieldPath,
}: {
  label: string;
  streamFieldPath: (fieldPath: string) => string;
}): JSX.Element => {
  const { formatMessage } = useIntl();

  return (
    <ToggleGroupField<RequestOption>
      label={formatMessage({ id: "connectorBuilder.injection.label" }, { label })}
      tooltip={formatMessage({ id: "connectorBuilder.injection.tooltip" }, { label: lowerCase(label) })}
      fieldPath={streamFieldPath("paginator.pageSizeOption")}
      initialValues={{
        inject_into: "request_parameter",
        type: "RequestOption",
        field_name: "",
      }}
    >
      <BuilderRequestInjection
        path={streamFieldPath("paginator.pageSizeOption")}
        descriptor={lowerCase(label)}
        excludeValues={["path"]}
      />
    </ToggleGroupField>
  );
};
