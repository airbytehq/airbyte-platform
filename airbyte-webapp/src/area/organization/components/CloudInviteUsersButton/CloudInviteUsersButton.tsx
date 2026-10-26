import { lazy, Suspense } from "react";

import { useIsCloudApp } from "core/utils/app";

const LazyInviteUsersButton = lazy(() =>
  import("cloud/views/users/InviteUsersButton").then(({ InviteUsersButton }) => ({ default: InviteUsersButton }))
);

export const CloudInviteUsersButton: React.FC<{ disabled?: boolean }> = ({ disabled }) => {
  return useIsCloudApp() ? (
    <Suspense fallback={null}>
      <LazyInviteUsersButton disabled={disabled} />
    </Suspense>
  ) : null;
};
