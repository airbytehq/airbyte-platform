import React, { Suspense } from "react";

import { Spinner } from "components/ui/Spinner";

import { InlineErrorBoundary } from "./InlineErrorBoundary";
import { OrganizationName } from "./OrganizationName";
import { WorkspaceName } from "./WorkspaceName";
import { StoreScope } from "../types";

// A spinner stands in for the name while it loads, and the scope id when it cannot be fetched. Either way, only
// this part of the row waits or fails, and the rest of the page stays as it is.
export const ScopeName: React.FC<{ scope: StoreScope }> = ({ scope }) => (
  <InlineErrorBoundary fallback={scope.id}>
    <Suspense fallback={<Spinner size="xs" />}>
      {scope.type === "workspace" ? (
        <WorkspaceName workspaceId={scope.id} />
      ) : (
        <OrganizationName organizationId={scope.id} />
      )}
    </Suspense>
  </InlineErrorBoundary>
);
