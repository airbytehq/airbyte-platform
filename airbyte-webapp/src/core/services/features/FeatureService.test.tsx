import { render, renderHook } from "@testing-library/react";
import React, { useEffect } from "react";

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
      expect(getFeature(FeatureItem.AllowUpdateConnectors)).toBe(false);
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
        overwrite: {} as FeatureSet,
      });
      expect(result.current.sort()).toEqual([FeatureItem.AllowDBTCloudIntegration]);
      rerender({ overwrite: undefined });
      expect(result.current.sort()).toEqual([FeatureItem.AllowDBTCloudIntegration]);
    });

    describe("entitlement overwrites", () => {
      it("should allow setting entitlement overwrites", () => {
        const { result } = renderHook(
          () => {
            const { features, setEntitlementOverwrites } = useFeatureService();
            useEffect(() => {
              setEntitlementOverwrites({ [FeatureItem.AllowUploadCustomImage]: true });
            }, [setEntitlementOverwrites]);
            return features;
          },
          { wrapper }
        );
        expect(result.current.sort()).toEqual([
          FeatureItem.AllowDBTCloudIntegration,
          FeatureItem.AllowUploadCustomImage,
        ]);
      });

      it("should allow clearing entitlement overwrites", () => {
        const { result, rerender } = renderHook(
          ({ overwrite }: { overwrite?: FeatureSet }) => {
            const { features, setEntitlementOverwrites } = useFeatureService();
            useEffect(() => {
              setEntitlementOverwrites(overwrite ?? {});
            }, [overwrite, setEntitlementOverwrites]);
            return features;
          },
          {
            wrapper,
            initialProps: {
              overwrite: { [FeatureItem.AllowUploadCustomImage]: true } as FeatureSet,
            },
          }
        );
        expect(result.current.sort()).toEqual([
          FeatureItem.AllowDBTCloudIntegration,
          FeatureItem.AllowUploadCustomImage,
        ]);
        rerender({ overwrite: {} as FeatureSet });
        expect(result.current.sort()).toEqual([FeatureItem.AllowDBTCloudIntegration]);
      });

      it("should merge feature flag and entitlement overwrites", () => {
        const { result } = renderHook(
          () => {
            const { features, setFeatureOverwrites, setEntitlementOverwrites } = useFeatureService();
            useEffect(() => {
              setFeatureOverwrites({ [FeatureItem.AllowUploadCustomImage]: true });
              setEntitlementOverwrites({ [FeatureItem.AllowUpdateConnectors]: true });
            }, [setFeatureOverwrites, setEntitlementOverwrites]);
            return features;
          },
          { wrapper }
        );
        expect(result.current.sort()).toEqual([
          FeatureItem.AllowDBTCloudIntegration,
          FeatureItem.AllowUpdateConnectors,
          FeatureItem.AllowUploadCustomImage,
        ]);
      });

      it("should prioritize feature flag overwrites over entitlement overwrites when ff disabled", () => {
        const { result } = renderHook(
          () => {
            const { features, setFeatureOverwrites, setEntitlementOverwrites } = useFeatureService();
            useEffect(() => {
              setFeatureOverwrites({ [FeatureItem.AllowDBTCloudIntegration]: false });
              setEntitlementOverwrites({ [FeatureItem.AllowDBTCloudIntegration]: true });
            }, [setFeatureOverwrites, setEntitlementOverwrites]);
            return features;
          },
          { wrapper }
        );
        expect(result.current.sort()).toEqual([]);
      });

      it("should prioritize feature flag overwrites over entitlement overwrites when ff enabled", () => {
        const { result } = renderHook(
          () => {
            const { features, setFeatureOverwrites, setEntitlementOverwrites } = useFeatureService();
            useEffect(() => {
              setFeatureOverwrites({ [FeatureItem.AllowDBTCloudIntegration]: true });
              setEntitlementOverwrites({ [FeatureItem.AllowDBTCloudIntegration]: false });
            }, [setFeatureOverwrites, setEntitlementOverwrites]);
            return features;
          },
          { wrapper }
        );
        expect(result.current.sort()).toEqual([FeatureItem.AllowDBTCloudIntegration]);
      });
    });

    describe("priority system", () => {
      beforeEach(() => {
        // Use a feature that we can control via env variable for testing
        process.env.REACT_APP_FEATURE_ALLOW_UPDATE_CONNECTORS = "true";
        (process.env.NODE_ENV as string) = "development";
      });

      afterEach(() => {
        process.env.REACT_APP_FEATURE_ALLOW_UPDATE_CONNECTORS = undefined;
        (process.env.NODE_ENV as string) = "test";
      });

      it("should prioritize feature flag overwrites over entitlement overwrites", () => {
        const { result } = renderHook(
          () => {
            const { features, setFeatureOverwrites, setEntitlementOverwrites } = useFeatureService();
            useEffect(() => {
              // Set entitlement to true
              setEntitlementOverwrites({ [FeatureItem.AllowUploadCustomImage]: true });
              // Set feature flag to false (should take precedence)
              setFeatureOverwrites({ [FeatureItem.AllowUploadCustomImage]: false });
            }, [setFeatureOverwrites, setEntitlementOverwrites]);
            return features;
          },
          { wrapper }
        );
        // Should not include the feature because feature flag (higher priority) is false
        expect(result.current).not.toContain(FeatureItem.AllowUploadCustomImage);
      });

      it("should fall back to entitlement when feature flag is not set", () => {
        const { result } = renderHook(
          () => {
            const { features, setEntitlementOverwrites } = useFeatureService();
            useEffect(() => {
              // Only set entitlement to true
              setEntitlementOverwrites({ [FeatureItem.AllowUploadCustomImage]: true });
              // Don't set feature flag
            }, [setEntitlementOverwrites]);
            return features;
          },
          { wrapper }
        );
        // Should include the feature because entitlement is true
        expect(result.current).toContain(FeatureItem.AllowUploadCustomImage);
      });

      it("should prioritize env variable over feature flags and entitlements", () => {
        const { result } = renderHook(
          () => {
            const { features, setFeatureOverwrites, setEntitlementOverwrites } = useFeatureService();
            useEffect(() => {
              // Set entitlement to false
              setEntitlementOverwrites({ [FeatureItem.AllowUpdateConnectors]: false });
              // Set feature flag to false
              setFeatureOverwrites({ [FeatureItem.AllowUpdateConnectors]: false });
              // Env variable is set to "true" in beforeEach (highest priority)
            }, [setFeatureOverwrites, setEntitlementOverwrites]);
            return features;
          },
          { wrapper }
        );
        // Should include the feature because env variable (highest priority) is true
        expect(result.current).toContain(FeatureItem.AllowUpdateConnectors);
      });
    });

    describe("env variable overwrites", () => {
      beforeEach(() => {
        process.env.REACT_APP_FEATURE_ALLOW_SYNC = "false";
        process.env.REACT_APP_FEATURE_ALLOW_CHANGE_DATAPLANES = "true";
      });

      afterEach(() => {
        (process.env.NODE_ENV as string) = "test";
        process.env.REACT_APP_FEATURE_ALLOW_SYNC = undefined;
        process.env.REACT_APP_FEATURE_ALLOW_CHANGE_DATAPLANES = undefined;
      });

      it("should allow overwriting it in dev", () => {
        (process.env.NODE_ENV as string) = "development";
        const getFeature = (feature: FeatureItem) => renderHook(() => useFeature(feature), { wrapper }).result.current;
        expect(getFeature(FeatureItem.AllowChangeDataplanes)).toBe(true);
      });

      it("should not overwrite in a non dev environment", () => {
        (process.env.NODE_ENV as string) = "production";
        const getFeature = (feature: FeatureItem) => renderHook(() => useFeature(feature), { wrapper }).result.current;
        expect(getFeature(FeatureItem.AllowChangeDataplanes)).toBe(false);
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
        <IfFeatureEnabled feature={FeatureItem.AllowUpdateConnectors}>
          <span data-testid="content" />
        </IfFeatureEnabled>,
        { wrapper }
      );
      expect(queryByTestId("content")).toBeFalsy();
    });

    it("allows changing features and rerenders correctly", () => {
      const { queryByTestId, rerender } = render(
        <FeatureService features={[FeatureItem.AllowDBTCloudIntegration]}>
          <IfFeatureEnabled feature={FeatureItem.AllowUpdateConnectors}>
            <span data-testid="content" />
          </IfFeatureEnabled>
        </FeatureService>
      );
      expect(queryByTestId("content")).toBeFalsy();
      rerender(
        <FeatureService features={[FeatureItem.AllowUpdateConnectors]}>
          <IfFeatureEnabled feature={FeatureItem.AllowUpdateConnectors}>
            <span data-testid="content" />
          </IfFeatureEnabled>
        </FeatureService>
      );
      expect(queryByTestId("content")).toBeTruthy();
    });
  });
});
