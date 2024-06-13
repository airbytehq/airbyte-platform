import { HttpError, useBuilderResolveManifestQuery } from "core/api";
import { ResolveManifest } from "core/api/types/ConnectorBuilderClient";
import { ConnectorManifest } from "core/api/types/ConnectorManifest";

import { CDK_VERSION } from "./cdk";
import { convertToBuilderFormValuesSync, ManifestCompatibilityError } from "./convertManifestToBuilderForm";
import { OLDEST_SUPPORTED_CDK_VERSION, versionSupported } from "./types";

export const useManifestToBuilderForm = () => {
  const resolve = useBuilderResolveManifestQuery();
  return { convertToBuilderFormValues: convertToBuilderFormValues.bind(this, resolve) };
};

export const convertToBuilderFormValues = async (
  resolve: (manifest: ConnectorManifest, projectId?: string) => Promise<ResolveManifest>,
  manifest: ConnectorManifest,
  projectId?: string
) => {
  let resolveResult: ResolveManifest;
  try {
    resolveResult = await resolve(manifest, projectId);
  } catch (e) {
    let errorMessage = e instanceof HttpError ? e.response.message : e.message;
    if (errorMessage[0] === '"') {
      errorMessage = errorMessage.substring(1, errorMessage.length);
    }
    if (errorMessage.slice(-1) === '"') {
      errorMessage = errorMessage.substring(0, errorMessage.length - 1);
    }
    throw new ManifestCompatibilityError(undefined, errorMessage.trim());
  }
  const resolvedManifest = resolveResult.manifest as ConnectorManifest;

  if (!versionSupported(resolvedManifest.version)) {
    throw new ManifestCompatibilityError(
      undefined,
      `Connector builder UI only supports manifests version >= ${OLDEST_SUPPORTED_CDK_VERSION} and <= ${CDK_VERSION}, encountered ${resolvedManifest.version}`
    );
  }

  return convertToBuilderFormValuesSync(resolvedManifest);
};
