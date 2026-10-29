import React, { useContext, useLayoutEffect } from "react";

import { AlertBanner } from "components/ui/Banner/AlertBanner";

import { BannerGroupContext } from "../../BannerGroupContext";

type HideableBannerProps = Omit<React.ComponentProps<typeof AlertBanner>, "color"> & { id: string };

// A banner that CloudBannerGroup hides while it is collapsed. Only info banners are hideable, so the color is fixed.
export const HideableBanner: React.FC<HideableBannerProps> = ({ id, ...alertBannerProps }) => {
  const { registerBanner, unregisterBanner, checkIsBannerHidden } = useContext(BannerGroupContext);

  useLayoutEffect(() => {
    registerBanner(id);

    return () => unregisterBanner(id);
  }, [id, registerBanner, unregisterBanner]);

  if (checkIsBannerHidden(id)) {
    return null;
  }

  return <AlertBanner {...alertBannerProps} />;
};
