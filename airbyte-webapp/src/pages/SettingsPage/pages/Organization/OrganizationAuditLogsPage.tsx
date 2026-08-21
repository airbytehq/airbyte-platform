import { createColumnHelper } from "@tanstack/react-table";
import dayjs from "dayjs";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { LoadingPage } from "components";
import { Badge } from "components/ui/Badge";
import { Button } from "components/ui/Button";
import { CopyButton } from "components/ui/CopyButton";
import { DataLoadingError } from "components/ui/DataLoadingError";
import DatePicker from "components/ui/DatePicker";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Option } from "components/ui/forms";
import { Heading } from "components/ui/Heading";
import { ListBox } from "components/ui/ListBox";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Table } from "components/ui/Table";
import { Text } from "components/ui/Text";

import { useCurrentOrganizationId } from "area/organization/utils";
import { AuditLogListFilters, useListAuditLogs, useListWorkspacesInOrganization } from "core/api";
import { AuditLogRead } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useModalService } from "core/services/Modal";

import styles from "./OrganizationAuditLogsPage.module.scss";

// The workspace filter dropdown lists the first page of an org's workspaces; orgs with more than
// this many workspaces can still filter by pasting a workspace id is not supported in v1.
const WORKSPACE_FILTER_PAGE_SIZE = 100;

// The whole page reads and writes UTC: DatePicker emits the picked wall-clock time labelled as
// UTC, and entries are bucketed by UTC date in storage. Formatting timestamps in the browser
// timezone instead would show times that do not match the range the filters selected.
const TIME_ZONE = "UTC";

/**
 * Turns a DatePicker value into an ISO instant, or undefined when it is not a usable date.
 *
 * DatePicker forwards raw input on every keystroke, so this sees partially typed values as the
 * user edits the field. `new Date(partial).toISOString()` throws a RangeError on those, which
 * would crash the page during render. Parsed as UTC to match what DatePicker's withTime mode
 * emits — `new Date()` would apply the browser offset to a value with no zone.
 */
const toIsoInstant = (value: string): string | undefined => {
  const parsed = dayjs.utc(value);
  return parsed.isValid() ? parsed.toISOString() : undefined;
};

type StatusFilter = "all" | "success" | "failed";

const columnHelper = createColumnHelper<AuditLogRead>();

export const OrganizationAuditLogsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION_AUDIT_LOGS);
  const { formatMessage, formatDate, formatTime } = useIntl();
  const organizationId = useCurrentOrganizationId();
  const { openModal } = useModalService();

  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [workspaceId, setWorkspaceId] = useState<string>("");
  const [actorId, setActorId] = useState("");
  const [operation, setOperation] = useState("");
  const [status, setStatus] = useState<StatusFilter>("all");

  // Token-based pagination: a stack of the page tokens used to reach each page. An empty stack
  // is the first page; "next" pushes the current page's nextPageToken, "previous" pops.
  const [pageTokens, setPageTokens] = useState<string[]>([]);
  const pageToken = pageTokens.length > 0 ? pageTokens[pageTokens.length - 1] : undefined;

  // The filters that bound which entries exist at all, as opposed to which of them the actor and
  // operation dropdowns narrow to. Kept separate because the dropdown options are derived from a
  // query that applies only these — see `optionSourceQuery`.
  const scopeFilters: AuditLogListFilters = useMemo(
    () => ({
      startTime: toIsoInstant(startDate),
      endTime: toIsoInstant(endDate),
      workspaceId: workspaceId || undefined,
      success: status === "all" ? undefined : status === "success",
    }),
    [startDate, endDate, workspaceId, status]
  );

  const filters: AuditLogListFilters = useMemo(
    () => ({
      ...scopeFilters,
      actorId: actorId || undefined,
      operation: operation || undefined,
    }),
    [scopeFilters, actorId, operation]
  );

  // Return to the first page whenever the filters change.
  useEffect(() => {
    setPageTokens([]);
  }, [filters]);

  // Source for the actor and operation dropdown options. This deliberately omits the actor and
  // operation filters: deriving the options from the filtered list would collapse each dropdown to
  // the single value already selected, leaving no way to switch to another one. With no actor or
  // operation selected it resolves to the same query key as the list below, so it costs no extra
  // request in the common case.
  const optionSourceQuery = useListAuditLogs(scopeFilters);
  const optionSourceEntries = useMemo(() => optionSourceQuery.data?.auditLogs ?? [], [optionSourceQuery.data]);

  const { data, isLoading, isError } = useListAuditLogs(filters, pageToken);

  const workspacesQuery = useListWorkspacesInOrganization({
    organizationId,
    pagination: { pageSize: WORKSPACE_FILTER_PAGE_SIZE, rowOffset: 0 },
  });
  const workspaces = useMemo(
    () => workspacesQuery.data?.pages.flatMap((page) => page.workspaces) ?? [],
    [workspacesQuery.data?.pages]
  );
  const workspaceNameById = useMemo(
    () => new Map(workspaces.map((workspace) => [workspace.workspaceId, workspace.name])),
    [workspaces]
  );

  const workspaceOptions: Array<Option<string>> = [
    { label: formatMessage({ id: "settings.organization.auditLogs.workspace.all" }), value: "" },
    ...workspaces.map((workspace) => ({ label: workspace.name, value: workspace.workspaceId })),
  ];

  const statusOptions: Array<Option<StatusFilter>> = [
    { label: formatMessage({ id: "settings.organization.auditLogs.status.all" }), value: "all" },
    { label: formatMessage({ id: "settings.organization.auditLogs.status.success" }), value: "success" },
    { label: formatMessage({ id: "settings.organization.auditLogs.status.failed" }), value: "failed" },
  ];

  // The actor filter matches on id or email, so send whichever the entry carries but label the
  // option with the email, matching what the actor column renders.
  const actorOptions: Array<Option<string>> = useMemo(() => {
    const byValue = new Map<string, string>();
    optionSourceEntries.forEach((entry) => {
      const value = entry.actor?.email ?? entry.actor?.actorId;
      if (value) {
        byValue.set(value, value);
      }
    });
    return [
      { label: formatMessage({ id: "settings.organization.auditLogs.actor.all" }), value: "" },
      ...[...byValue.keys()].sort().map((value) => ({ label: value, value })),
    ];
  }, [optionSourceEntries, formatMessage]);

  const operationOptions: Array<Option<string>> = useMemo(
    () => [
      { label: formatMessage({ id: "settings.organization.auditLogs.operation.all" }), value: "" },
      ...[...new Set(optionSourceEntries.map((entry) => entry.operation))]
        .sort()
        .map((value) => ({ label: value, value })),
    ],
    [optionSourceEntries, formatMessage]
  );

  // The table surfaces a summary of each entry; the modal is the escape hatch to everything the
  // API returned for it, including the request and response payloads.
  const openDetails = useCallback(
    (entry: AuditLogRead) => {
      const rawEntry = JSON.stringify(entry, null, 2);
      openModal({
        title: formatMessage({ id: "settings.organization.auditLogs.details.title" }, { operation: entry.operation }),
        size: "lg",
        testId: "audit-log-details-modal",
        content: () => (
          <>
            <ModalBody>
              <pre className={styles.rawEntry} data-testid="audit-log-raw-entry">
                {rawEntry}
              </pre>
            </ModalBody>
            <ModalFooter>
              <CopyButton content={rawEntry}>
                <FormattedMessage id="copyButton.title" />
              </CopyButton>
            </ModalFooter>
          </>
        ),
      });
    },
    [formatMessage, openModal]
  );

  const columns = useMemo(
    () => [
      columnHelper.accessor("timestamp", {
        header: formatMessage({ id: "settings.organization.auditLogs.table.timestamp" }),
        // Rendered in UTC, not the browser timezone, so the times here line up with the range the
        // date filters select: DatePicker emits the picked wall-clock time labelled as UTC, and
        // entries are bucketed in storage by their UTC date.
        cell: (info) =>
          `${formatDate(info.getValue(), { dateStyle: "short", timeZone: TIME_ZONE })} ${formatTime(info.getValue(), {
            timeStyle: "medium",
            timeZone: TIME_ZONE,
          })}`,
        enableSorting: false,
      }),
      columnHelper.accessor((row) => row.actor?.email ?? row.actor?.actorId ?? "—", {
        id: "actor",
        header: formatMessage({ id: "settings.organization.auditLogs.table.actor" }),
        cell: (info) => info.getValue(),
        enableSorting: false,
      }),
      columnHelper.accessor("operation", {
        header: formatMessage({ id: "settings.organization.auditLogs.table.operation" }),
        enableSorting: false,
      }),
      columnHelper.accessor(
        (row) => (row.workspaceId ? workspaceNameById.get(row.workspaceId) ?? row.workspaceId : "—"),
        {
          id: "workspace",
          header: formatMessage({ id: "settings.organization.auditLogs.table.workspace" }),
          cell: (info) => info.getValue(),
          enableSorting: false,
        }
      ),
      columnHelper.accessor("success", {
        header: formatMessage({ id: "settings.organization.auditLogs.table.status" }),
        cell: (info) =>
          info.getValue() ? (
            <Badge variant="green">{formatMessage({ id: "settings.organization.auditLogs.status.success" })}</Badge>
          ) : (
            <Badge variant="red">{formatMessage({ id: "settings.organization.auditLogs.status.failed" })}</Badge>
          ),
        enableSorting: false,
      }),
      columnHelper.accessor((row) => row.errorMessage ?? "", {
        id: "errorMessage",
        header: formatMessage({ id: "settings.organization.auditLogs.table.error" }),
        cell: (info) => info.getValue(),
        enableSorting: false,
      }),
    ],
    [formatMessage, formatDate, formatTime, workspaceNameById]
  );

  if (isError) {
    return (
      <DataLoadingError>
        <FormattedMessage id="settings.organization.auditLogs.error" />
      </DataLoadingError>
    );
  }

  if (isLoading) {
    return <LoadingPage />;
  }

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1" size="md">
        <FormattedMessage id="settings.organization.auditLogs.title" />
      </Heading>

      <FlexContainer gap="sm" alignItems="flex-end" className={styles.filters}>
        <FlexItem>
          <DatePicker
            withTime
            value={startDate}
            onChange={setStartDate}
            placeholder={formatMessage({ id: "settings.organization.auditLogs.filters.startTime" })}
          />
        </FlexItem>
        <FlexItem>
          <DatePicker
            withTime
            value={endDate}
            onChange={setEndDate}
            placeholder={formatMessage({ id: "settings.organization.auditLogs.filters.endTime" })}
          />
        </FlexItem>
        <FlexItem>
          <ListBox
            options={workspaceOptions}
            selectedValue={workspaceId}
            onSelect={setWorkspaceId}
            placeholder={formatMessage({ id: "settings.organization.auditLogs.filters.workspace" })}
            data-testid="audit-logs-workspace-filter"
          />
        </FlexItem>
        <FlexItem>
          <ListBox
            options={actorOptions}
            selectedValue={actorId}
            onSelect={setActorId}
            placeholder={formatMessage({ id: "settings.organization.auditLogs.filters.actor" })}
            data-testid="audit-logs-actor-filter"
          />
        </FlexItem>
        <FlexItem>
          <ListBox
            options={operationOptions}
            selectedValue={operation}
            onSelect={setOperation}
            placeholder={formatMessage({ id: "settings.organization.auditLogs.filters.operation" })}
            data-testid="audit-logs-operation-filter"
          />
        </FlexItem>
        <FlexItem>
          <ListBox
            options={statusOptions}
            selectedValue={status}
            onSelect={setStatus}
            data-testid="audit-logs-status-filter"
          />
        </FlexItem>
      </FlexContainer>

      <Table
        testId="audit-logs-table"
        columns={columns}
        data={data?.auditLogs ?? []}
        rowId="id"
        sorting={false}
        onClickRow={openDetails}
        customEmptyPlaceholder={<FormattedMessage id="settings.organization.auditLogs.empty" />}
      />

      <FlexContainer justifyContent="flex-end" alignItems="center" gap="sm">
        <Button
          variant="clear"
          disabled={pageTokens.length === 0}
          onClick={() => setPageTokens((tokens) => tokens.slice(0, -1))}
          data-testid="audit-logs-previous-page"
        >
          <FormattedMessage id="settings.organization.auditLogs.pagination.previous" />
        </Button>
        <Text>
          <FormattedMessage
            id="settings.organization.auditLogs.pagination.page"
            values={{ page: pageTokens.length + 1 }}
          />
        </Text>
        <Button
          variant="clear"
          disabled={!data?.nextPageToken}
          onClick={() => {
            const token = data?.nextPageToken;
            if (token) {
              setPageTokens((tokens) => [...tokens, token]);
            }
          }}
          data-testid="audit-logs-next-page"
        >
          <FormattedMessage id="settings.organization.auditLogs.pagination.next" />
        </Button>
      </FlexContainer>
    </FlexContainer>
  );
};

export default OrganizationAuditLogsPage;
