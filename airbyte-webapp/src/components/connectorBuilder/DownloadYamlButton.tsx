import { dump } from "js-yaml";
import snakeCase from "lodash/snakeCase";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";
import { Tooltip } from "components/ui/Tooltip";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { FILE_TYPE_DOWNLOAD, downloadFile } from "core/utils/file";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./DownloadYamlButton.module.scss";
import { useBuilderWatch } from "./types";
import { useBuilderErrors } from "./useBuilderErrors";

interface DownloadYamlButtonProps {
  className?: string;
}

export const DownloadYamlButton: React.FC<DownloadYamlButtonProps> = ({ className }) => {
  const analyticsService = useAnalyticsService();
  const { validateAndTouch } = useBuilderErrors();
  const connectorNameField = useBuilderWatch("name");
  const { jsonManifest, yamlIsValid, formValuesValid } = useConnectorBuilderFormState();
  const yaml = useBuilderWatch("yaml");
  const mode = useBuilderWatch("mode");

  const downloadYaml = () => {
    const yamlToDownload =
      mode === "ui"
        ? dump(jsonManifest, {
            noRefs: true,
          })
        : yaml;
    const file = new Blob([yamlToDownload], { type: FILE_TYPE_DOWNLOAD });
    downloadFile(file, connectorNameField ? `${snakeCase(connectorNameField)}.yaml` : "connector_builder.yaml");
    analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.DOWNLOAD_YAML, {
      actionDescription: "User clicked the Download Config button to download the YAML manifest",
      editor_view: mode,
    });
  };

  const handleClick = () => {
    if (mode === "yaml") {
      downloadYaml();
      return;
    }

    validateAndTouch(downloadYaml);
  };

  let buttonDisabled = false;
  let showWarningIcon = false;
  let tooltipContent = undefined;

  if (mode === "yaml" && !yamlIsValid) {
    buttonDisabled = true;
    showWarningIcon = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.invalidYamlDownload" />;
  }

  if (mode === "ui" && !formValuesValid) {
    showWarningIcon = true;
    tooltipContent = <FormattedMessage id="connectorBuilder.configErrorsDownload" />;
  }

  const downloadButton = (
    <Button
      full
      variant="secondary"
      onClick={handleClick}
      disabled={buttonDisabled}
      icon={showWarningIcon ? <Icon type="warningOutline" /> : undefined}
      data-testid="download-yaml-button"
      type="button"
    >
      <FormattedMessage id="connectorBuilder.downloadYaml" />
    </Button>
  );

  return (
    <div className={className}>
      {tooltipContent !== undefined ? (
        <Tooltip
          control={downloadButton}
          placement={mode === "yaml" ? "left" : "top"}
          containerClassName={styles.tooltipContainer}
        >
          {tooltipContent}
        </Tooltip>
      ) : (
        downloadButton
      )}
    </div>
  );
};
