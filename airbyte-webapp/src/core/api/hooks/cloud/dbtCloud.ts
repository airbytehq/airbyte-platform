// This module is for the business logic of working with dbt Cloud webhooks.
// Static config data, urls, functions which wrangle the APIs to manipulate
// records in ways suited to the UI user workflows--all the implementation
// details of working with dbtCloud jobs as webhook operations, all goes here.
// The presentation logic and orchestration in the UI all goes elsewhere.
//
// About that business logic:
// - for now, the code treats "webhook operations" and "dbt Cloud job" as synonymous.
// - custom domains aren't yet supported

import { useMutation, useQuery } from "@tanstack/react-query";
import isEmpty from "lodash/isEmpty";
import { useIntl } from "react-intl";
import { useAsyncFn } from "react-use";

import {
  OperatorType,
  WebBackendConnectionRead,
  OperationRead,
  OperatorWebhookWebhookType,
  WebhookConfigRead,
  WorkspaceRead,
  WebhookConfigWrite,
} from "core/api/types/AirbyteClient";
import { useRequestOptions } from "core/api/useRequestOptions";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useNotificationService } from "hooks/services/Notification";

import { webBackendGetAvailableDbtJobsForWorkspace } from "../../generated/CloudApi";
import { DbtCloudJobInfo, WorkspaceGetDbtJobsResponse } from "../../types/CloudApi";
import { useCurrentWorkspace, useUpdateWorkspace } from "../workspaces";

export interface DbtCloudJob {
  accountId: number;
  jobId: number;
  operationId?: string;
  jobName?: string;
}

type ServiceToken = string;

const WEBHOOK_CONFIG_NAME = "dbt cloud";

const jobName = (t: DbtCloudJob) => `${t.accountId}/${t.jobId}`;

const isDbtWebhookConfig = (webhookConfig: WebhookConfigRead) => !!webhookConfig.name?.includes("dbt");

const hasDbtIntegration = (workspace: WorkspaceRead) => !isEmpty(workspace.webhookConfigs?.filter(isDbtWebhookConfig));

export const toDbtCloudJob = (operationRead: OperationRead): DbtCloudJob => {
  if (operationRead.operatorConfiguration.webhook?.webhookType === "dbtCloud") {
    const dbtCloud = operationRead.operatorConfiguration.webhook.dbtCloud as DbtCloudJob;
    return {
      accountId: dbtCloud.accountId,
      jobId: dbtCloud.jobId,
    };
  }
  throw new Error(
    `Cannot convert operationRead of type ${operationRead.operatorConfiguration.operatorType} to DbtCloudJob`
  );
};

export const isDbtCloudJob = (operation: OperationRead): boolean =>
  operation.operatorConfiguration.operatorType === OperatorType.webhook;

export const isSameJob = (remoteJob: DbtCloudJobInfo, savedJob: DbtCloudJob): boolean =>
  savedJob.accountId === remoteJob.accountId && savedJob.jobId === remoteJob.jobId;

export const useDbtCloudServiceToken = () => {
  const workspace = useCurrentWorkspace();
  const { workspaceId } = workspace;
  const { mutateAsync: updateWorkspace } = useUpdateWorkspace();

  const { mutateAsync: saveToken, isLoading: isSavingToken } = useMutation<WorkspaceRead, Error, ServiceToken>(
    ["submitWorkspaceDbtCloudToken", workspaceId],
    async (authToken: string) => {
      const webhookConfigs = [
        {
          name: WEBHOOK_CONFIG_NAME,
          authToken,
        },
      ];

      return await updateWorkspace({
        workspaceId,
        webhookConfigs,
      });
    }
  );

  const { mutateAsync: deleteToken, isLoading: isDeletingToken } = useMutation<WorkspaceRead, Error>(
    ["submitWorkspaceDbtCloudToken", workspaceId],
    async () => {
      const webhookConfigs: WebhookConfigWrite[] = [];

      return await updateWorkspace({
        workspaceId,
        webhookConfigs,
      });
    }
  );

  return {
    hasExistingToken: hasDbtIntegration(workspace),
    saveToken,
    isSavingToken,
    deleteToken,
    isDeletingToken,
  };
};

export const useDbtIntegration = (connection: WebBackendConnectionRead) => {
  const workspace = useCurrentWorkspace();
  const { workspaceId } = workspace;

  const webhookConfigId = workspace.webhookConfigs?.find((config) => isDbtWebhookConfig(config))?.id;

  const dbtCloudJobs = [...(connection.operations?.filter((operation) => isDbtCloudJob(operation)) || [])].map(
    toDbtCloudJob
  );
  const otherOperations = [...(connection.operations?.filter((operation) => !isDbtCloudJob(operation)) || [])];
  const { registerNotification } = useNotificationService();

  const { formatMessage } = useIntl();

  const { updateConnection } = useConnectionEditService();
  const [{ loading }, saveJobs] = useAsyncFn(
    async (jobs: DbtCloudJob[]) => {
      try {
        await updateConnection({
          connectionId: connection.connectionId,
          operations: [
            ...otherOperations,
            ...jobs.map((job) => ({
              workspaceId,
              ...(job.operationId ? { operationId: job.operationId } : {}),
              name: jobName(job),
              operatorConfiguration: {
                operatorType: OperatorType.webhook,
                webhook: {
                  webhookType: OperatorWebhookWebhookType.dbtCloud,
                  dbtCloud: {
                    jobId: job.jobId,
                    accountId: job.accountId,
                  },
                  // if `hasDbtIntegration` is true, webhookConfigId is guaranteed to exist
                  ...(webhookConfigId ? { webhookConfigId } : {}),
                },
              },
            })),
          ],
        });
      } catch (e) {
        // FIXME: remove this once we migrate to react-hook-form since it will handle onError and OnSuccess
        registerNotification({
          id: "connection.updateFailed",
          text: formatMessage({ id: "connection.updateFailed" }),
        });
      }
    },
    [connection, otherOperations, updateConnection, webhookConfigId, workspaceId]
  );

  return {
    hasDbtIntegration: hasDbtIntegration(workspace),
    dbtCloudJobs,
    saveJobs,
    isSaving: loading,
  };
};

export const useAvailableDbtJobs = () => {
  const requestOptions = useRequestOptions();
  const workspace = useCurrentWorkspace();
  const { workspaceId } = workspace;
  const dbtConfigId = workspace.webhookConfigs?.find((config) => config.name?.includes("dbt"))?.id;

  if (!dbtConfigId) {
    throw new Error("cannot request available dbt jobs for a workspace with no dbt cloud integration configured");
  }

  const results = useQuery(
    ["dbtCloud", dbtConfigId, "list"],
    () => webBackendGetAvailableDbtJobsForWorkspace({ workspaceId, dbtConfigId }, requestOptions),
    {
      suspense: true,
    }
  );

  // casting type to remove `| undefined`, since `suspense: true` will ensure the value
  // is, in fact, available
  return (results.data as WorkspaceGetDbtJobsResponse).availableDbtJobs;
};
