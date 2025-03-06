import { parse, print } from "graphql";

export const formatGraphqlQuery = (query: string): string => {
  // Returns a formatted Graphql query string.
  // If the "query" keyword was already present, we re-inject it at the beginning of the string
  // for consistency and clarity. Otherwise, we return the formatted query as is.
  if (query.startsWith("query")) {
    return `query ${print(parse(query))}`;
  }
  return `${print(parse(query))}`;
};
