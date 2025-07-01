import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { useDiagnosticReport } from "core/api";
import { DefaultErrorBoundary } from "core/errors";

const DiagnosticsButtonInner = () => {
  const downloadDiagnosticReport = useDiagnosticReport();
  const [isDownloading, setIsDownloading] = React.useState(false);
  const organizationId = useCurrentOrganizationId();
  const [error, setError] = React.useState<Error | null>(null);

  if (error) {
    throw error;
  }

  const runDiagnostics = async () => {
    setIsDownloading(true);

    const reportPromise = downloadDiagnosticReport(organizationId);
    const minWait = new Promise((resolve) => setTimeout(resolve, 1000));

    try {
      await Promise.all([reportPromise, minWait]);
    } catch (e) {
      setError(e);
    } finally {
      setIsDownloading(false);
    }
  };

  return (
    <Button
      disabled={isDownloading}
      isLoading={isDownloading}
      variant="secondary"
      icon="download"
      iconSize="sm"
      onClick={runDiagnostics}
    >
      <FormattedMessage id="settings.organization.diagnostics.download" />
    </Button>
  );
};

export const DiagnosticsButton = () => {
  return (
    <DefaultErrorBoundary>
      <DiagnosticsButtonInner />
    </DefaultErrorBoundary>
  );
};
