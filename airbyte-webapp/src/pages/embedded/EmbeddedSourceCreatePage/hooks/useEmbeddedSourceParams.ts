import { useSearchParams } from "react-router-dom";

export const WORKSPACE_ID_PARAM = "workspaceId";
export const SELECTED_TEMPLATE_ID_PARAM = "selectedTemplateId";
export const SELECTED_PARTIAL_CONFIG_ID_PARAM = "selectedPartialConfigId";
export const EDIT_PARTIAL_USER_CONFIG_PARAM = "editConfig";
export const ALLOWED_ORIGIN_PARAM = "allowedOrigin";

interface EmbeddedSourceParams {
  allowedOrigin: string | null;
  workspaceId: string;
  selectedTemplateId: string | null;
  selectedPartialConfigId: string | null;
  editPartialUserConfig: string | null;
  setEditConfig: (value: boolean) => void;
  setSelectedConfig: (partialUserConfigId: string) => void;
  setSelectedTemplate: (configTemplateId: string) => void;
  clearSelectedConfig: () => void;
  clearSelectedTemplate: () => void;
}

export const useEmbeddedSourceParams = (): EmbeddedSourceParams => {
  const [searchParams, setSearchParams] = useSearchParams();

  const workspaceId = searchParams.get(WORKSPACE_ID_PARAM) ?? "";
  const selectedTemplateId = searchParams.get(SELECTED_TEMPLATE_ID_PARAM);
  const selectedPartialConfigId = searchParams.get(SELECTED_PARTIAL_CONFIG_ID_PARAM);

  const editPartialUserConfig = searchParams.get(EDIT_PARTIAL_USER_CONFIG_PARAM);

  const allowedOrigin = searchParams.get(ALLOWED_ORIGIN_PARAM);

  const setEditConfig = (value: boolean) => {
    setSearchParams((params) => {
      if (value === true) {
        params.set(EDIT_PARTIAL_USER_CONFIG_PARAM, "true");
      } else {
        params.delete(EDIT_PARTIAL_USER_CONFIG_PARAM);
      }
      return params;
    });
  };

  const setSelectedConfig = (partialUserConfigId: string) => {
    setSearchParams((params) => {
      params.set(SELECTED_PARTIAL_CONFIG_ID_PARAM, partialUserConfigId);
      return params;
    });
  };

  const setSelectedTemplate = (configTemplateId: string) => {
    setSearchParams((params) => {
      params.set(SELECTED_TEMPLATE_ID_PARAM, configTemplateId);
      return params;
    });
  };

  const clearSelectedConfig = () => {
    setSearchParams((params) => {
      params.delete(SELECTED_PARTIAL_CONFIG_ID_PARAM);
      return params;
    });
  };

  const clearSelectedTemplate = () => {
    setSearchParams((params) => {
      params.delete(SELECTED_TEMPLATE_ID_PARAM);
      return params;
    });
  };

  return {
    allowedOrigin,
    workspaceId,
    selectedTemplateId,
    selectedPartialConfigId,
    editPartialUserConfig,
    setEditConfig,
    setSelectedConfig,
    setSelectedTemplate,
    clearSelectedConfig,
    clearSelectedTemplate,
  };
};
