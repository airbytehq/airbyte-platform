import { ExternalLink } from "components/ui/Link";

import { I18nError } from "core/errors";
import { links } from "core/utils/links";

export class FormBuildError extends I18nError {
  constructor(
    public message: string,
    public connectorDefinitionId?: string
  ) {
    super(message, {
      docLink: (node: React.ReactNode) => (
        <ExternalLink href={links.connectorSpecificationDocs} variant="primary">
          {node}
        </ExternalLink>
      ),
    });
    this.name = "FormBuildError";
  }
}

export function isFormBuildError(error: unknown): error is FormBuildError {
  return error instanceof FormBuildError;
}
