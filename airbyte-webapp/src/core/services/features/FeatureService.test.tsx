import { render, renderHook } from "@testing-library/react";
import React, { useEffect } from "react";

import { mockProInstanceConfig } from "test-utils/mock-data/mockInstanceConfig";

import { FeatureService, IfFeatureEnabled, useFeature, useFeatureService } from "./FeatureService";
import { FeatureItem, FeatureSet } from "./types";

const wrapper: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <FeatureService features={[FeatureItem.AllowDBTCloudIntegration]}>{children}</FeatureService>
);

type FeatureOverwrite = FeatureItem[] | FeatureSet | undefined;

interface FeatureOverwrites {
  overwrite?: FeatureOverwrite;
}

/**
 * Test utility method to wrap setting all the different level of features, rerender
 * with a different set of features and getting the merged feature set.
 */
const getFeatures = (initialProps: FeatureOverwrites) => {
  return renderHook(
    ({ overwrite }: React.PropsWithChildren<FeatureOverwrites>) => {
      const { features, setFeatureOverwrites } = useFeatureService();
      useEffect(() => {
        setFeatureOverwrites(overwrite);
      }, [overwrite, setFeatureOverwrites]);
      return features;
    },
    { wrapper, initialProps }
  );
};

describe("Feature Service", () => {
  describe("FeatureService", () => {
    it("should allow setting default features", () => {
      const getFeature = (feature: FeatureItem) => renderHook(() => useFeature(feature), { wrapper }).result.current;
      expect(getFeature(FeatureItem.AllowDBTCloudIntegration)).toBe(true);
      expect(getFeature(FeatureItem.AllowCustomDBT)).toBe(false);
      expect(getFeature(FeatureItem.AllowUpdateConnectors)).toBe(false);
    });

    it("should set features based on airbyte pro edition", () => {
      const wrapperWithInstanceConfig: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
        <FeatureService features={[FeatureItem.AllowDBTCloudIntegration]} instanceConfig={mockProInstanceConfig}>
          {children}
        </FeatureService>
      );
      const getFeature = (feature: FeatureItem) =>
        renderHook(() => useFeature(feature), { wrapper: wrapperWithInstanceConfig }).result.current;

      expect(getFeature(FeatureItem.KeycloakAuthentication)).toBe(true);
    });

    it("overwrite features can overwrite default features", () => {
      expect(
        getFeatures({
          overwrite: { [FeatureItem.AllowUploadCustomImage]: true, [FeatureItem.AllowDBTCloudIntegration]: false },
        }).result.current.sort()
      ).toEqual([FeatureItem.AllowUploadCustomImage]);
    });

    it("overwritten features can be cleared again", () => {
      const { result, rerender } = getFeatures({
        overwrite: { [FeatureItem.AllowCustomDBT]: true } as FeatureSet,
      });
      expect(result.current.sort()).toEqual([FeatureItem.AllowCustomDBT, FeatureItem.AllowDBTCloudIntegration]);
      rerender({ overwrite: undefined });
      expect(result.current.sort()).toEqual([FeatureItem.AllowDBTCloudIntegration]);
    });

    describe("env variable overwrites", () => {
      beforeEach(() => {
        process.env.REACT_APP_FEATURE_ALLOW_SYNC = "false";
        process.env.REACT_APP_FEATURE_ALLOW_CHANGE_DATA_GEOGRAPHIES = "true";
      });

      afterEach(() => {
        (process.env.NODE_ENV as string) = "test";
        process.env.REACT_APP_FEATURE_ALLOW_SYNC = undefined;
        process.env.REACT_APP_FEATURE_ALLOW_CHANGE_DATA_GEOGRAPHIES = undefined;
      });

      it("should allow overwriting it in dev", () => {
        (process.env.NODE_ENV as string) = "development";
        const getFeature = (feature: FeatureItem) => renderHook(() => useFeature(feature), { wrapper }).result.current;
        expect(getFeature(FeatureItem.AllowChangeDataGeographies)).toBe(true);
      });

      it("should not overwrite in a non dev environment", () => {
        (process.env.NODE_ENV as string) = "production";
        const getFeature = (feature: FeatureItem) => renderHook(() => useFeature(feature), { wrapper }).result.current;
        expect(getFeature(FeatureItem.AllowChangeDataGeographies)).toBe(false);
      });
    });
  });

  describe("IfFeatureEnabled", () => {
    it("renders its children if the given feature is enabled", () => {
      const { getByTestId } = render(
        <IfFeatureEnabled feature={FeatureItem.AllowDBTCloudIntegration}>
          <span data-testid="content" />
        </IfFeatureEnabled>,
        { wrapper }
      );
      expect(getByTestId("content")).toBeTruthy();
    });

    it("does not render its children if the given feature is disabled", () => {
      const { queryByTestId } = render(
        <IfFeatureEnabled feature={FeatureItem.AllowOAuthConnector}>
          <span data-testid="content" />
        </IfFeatureEnabled>,
        { wrapper }
      );
      expect(queryByTestId("content")).toBeFalsy();
    });

    it("allows changing features and rerenders correctly", () => {
      const { queryByTestId, rerender } = render(
        <FeatureService features={[FeatureItem.AllowDBTCloudIntegration]}>
          <IfFeatureEnabled feature={FeatureItem.AllowOAuthConnector}>
            <span data-testid="content" />
          </IfFeatureEnabled>
        </FeatureService>
      );
      expect(queryByTestId("content")).toBeFalsy();
      rerender(
        <FeatureService features={[FeatureItem.AllowOAuthConnector]}>
          <IfFeatureEnabled feature={FeatureItem.AllowOAuthConnector}>
            <span data-testid="content" />
          </IfFeatureEnabled>
        </FeatureService>
      );
      expect(queryByTestId("content")).toBeTruthy();
    });
  });
});
