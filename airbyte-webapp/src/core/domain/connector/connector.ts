import { DestinationDefinitionSpecificationRead, SourceDefinitionSpecificationRead } from "core/request/AirbyteClient";

import { isSource, isSourceDefinition, isSourceDefinitionSpecification } from "./source";
import { ConnectorDefinition, ConnectorT } from "./types";

export class Connector {
  static id(connector: ConnectorDefinition): string {
    return isSourceDefinition(connector) ? connector.sourceDefinitionId : connector.destinationDefinitionId;
  }
}

export class ConnectorHelper {
  static id(connector: ConnectorT): string {
    return isSource(connector) ? connector.sourceId : connector.destinationId;
  }
}

export class ConnectorSpecification {
  static id(connector: DestinationDefinitionSpecificationRead | SourceDefinitionSpecificationRead): string {
    return isSourceDefinitionSpecification(connector)
      ? connector.sourceDefinitionId
      : connector.destinationDefinitionId;
  }
}
