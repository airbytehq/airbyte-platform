import React, { useState } from "react";
import { FormattedMessage } from "react-intl";
import { useAsyncFn } from "react-use";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useUpdateWorkspace } from "core/api";

const useWorkspaceEditor = (): {
  updateData: (data: {
    email?: string;
    anonymousDataCollection: boolean;
    news: boolean;
    securityUpdates: boolean;
  }) => Promise<void>;
  errorMessage: React.ReactNode;
  successMessage: React.ReactNode;
  loading?: boolean;
} => {
  const workspaceId = useCurrentWorkspaceId();
  const { mutateAsync: updateWorkspace } = useUpdateWorkspace();
  const [errorMessage, setErrorMessage] = useState<React.ReactNode>(null);
  const [successMessage, setSuccessMessage] = useState<React.ReactNode>(null);

  const [{ loading }, updateData] = useAsyncFn(
    async (data: { news: boolean; securityUpdates: boolean; anonymousDataCollection: boolean; email?: string }) => {
      setErrorMessage(null);
      setSuccessMessage(null);
      try {
        await updateWorkspace({
          workspaceId,
          email: data.email,
          anonymousDataCollection: data.anonymousDataCollection,
          news: data.news,
          securityUpdates: data.securityUpdates,
        });
        setSuccessMessage(<FormattedMessage id="form.changesSaved" />);
      } catch (e) {
        setErrorMessage(<FormattedMessage id="form.someError" />);
      }
    },
    [setErrorMessage, setSuccessMessage]
  );

  return {
    updateData,
    errorMessage,
    successMessage,
    loading,
  };
};

export default useWorkspaceEditor;
