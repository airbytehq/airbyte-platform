import { parse, print } from "graphql";

export const formatGraphqlQuery = (query: string): string => {
  // Returns a formatted Graphql query string.
  // In most cases, we simply return the formatted query as is.
  // On local OSS instance, the "query" keyword is removed by the formatter.
  // When this happens, we re-inject it to not alter the user's input.

  const formattedQuery = print(parse(query));

  if (query.startsWith("query") && !formattedQuery.startsWith("query")) {
    return `query ${print(parse(query))}`;
  }

  return formattedQuery;
};
