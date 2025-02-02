import { CodeEditor } from "components/ui/CodeEditor";

const componentscode = `from dataclasses import InitVar, dataclass
from typing import Any, Mapping

from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator
from airbyte_cdk.sources.declarative.types import Config

@dataclass
class CustomBearerAuthenticator(DeclarativeAuthenticator):
    """
    Custom authenticator that uses "SSWS" instead of "Bearer" in the authorization header.
    """

    config: Config
    parameters: InitVar[Mapping[str, Any]]

    @property
    def auth_header(self) -> str:
        return "Authorization"

    @property
    def token(self) -> str:
        return f"SSWS {self.config['credentials']['api_token']}"`;

export const CustomComponentsEditor = () => {
  return <CodeEditor language="python" value={componentscode} lineNumberCharacterWidth={6} paddingTop />;
};
