import { useIntl } from "react-intl";

import { formatGraphqlQuery } from "components/ui/CodeEditor/GraphqlFormatter";

import { useNotificationService } from "hooks/services/Notification";

import { CodeEditor } from "./CodeEditor";

interface GraphQLEditorProps {
  value: string;
  onChange: (value: string | undefined) => void;
  disabled?: boolean;
  paddingTop?: boolean;
}

export const GraphQLEditor: React.FC<GraphQLEditorProps> = ({ value, onChange, disabled, paddingTop }) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();

  return (
    <CodeEditor
      value={value}
      onChange={onChange}
      language="graphql"
      disabled={disabled}
      paddingTop={paddingTop}
      options={{
        formatOnPaste: true,
        formatOnType: true,
        autoIndent: "full",
      }}
      beforeMount={(monaco) => {
        monaco.languages.registerDocumentFormattingEditProvider("graphql", {
          async provideDocumentFormattingEdits(model) {
            const text = model.getValue();
            try {
              const formattedQuery = formatGraphqlQuery(text);
              return [
                {
                  range: model.getFullModelRange(),
                  text: formattedQuery,
                },
              ];
            } catch (e) {
              registerNotification({
                type: "error",
                id: "graphqlQuery.formattingError",
                text: formatMessage({ id: "connectorBuilder.requestOptions.graphqlQuery.formattingError" }),
              });
              return [];
            }
          },
        });
      }}
    />
  );
};
