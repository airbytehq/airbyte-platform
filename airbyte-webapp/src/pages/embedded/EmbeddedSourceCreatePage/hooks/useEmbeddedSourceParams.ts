import { useSearchParams } from "react-router-dom";

export const WORKSPACE_ID_PARAM = "workspaceId";
export const SELECTED_TEMPLATE_ID_PARAM = "selectedTemplateId";
export const SELECTED_PARTIAL_CONFIG_ID_PARAM = "selectedPartialConfigId";
export const CREATE_PARTIAL_USER_CONFIG_PARAM = "createConfig";

export const useEmbeddedSourceParams = () => {
  const [searchParams, setSearchParams] = useSearchParams();

  const workspaceId = searchParams.get(WORKSPACE_ID_PARAM) ?? "";
  const selectedTemplateId = searchParams.get(SELECTED_TEMPLATE_ID_PARAM);
  const selectedPartialConfigId = searchParams.get(SELECTED_PARTIAL_CONFIG_ID_PARAM);
  const createPartialUserConfig = searchParams.get(CREATE_PARTIAL_USER_CONFIG_PARAM);

  const setCreateConfig = () => {
    setSearchParams((params) => {
      params.set(CREATE_PARTIAL_USER_CONFIG_PARAM, "true");
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

  return {
    workspaceId,
    selectedTemplateId,
    selectedPartialConfigId,
    createPartialUserConfig,
    setCreateConfig,
    setSelectedConfig,
    setSelectedTemplate,
  };
};
