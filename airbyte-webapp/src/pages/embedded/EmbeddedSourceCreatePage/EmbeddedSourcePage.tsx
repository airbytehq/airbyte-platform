import { useSearchParams } from "react-router-dom";

import { ModalBody } from "components/ui/Modal";

import { MaskCreateForm } from "./components/PartialUserConfigCreateForm";
import { TemplateSelectList } from "./components/TemplateSelectList";

export const EmbeddedSourceCreatePage: React.FC = () => {
  const [searchParams] = useSearchParams();

  const selectedTemplateId = searchParams.get("selectedTemplateId");

  return (
    <ModalBody>
      {!!selectedTemplateId && selectedTemplateId.length > 0 ? <MaskCreateForm /> : <TemplateSelectList />}
    </ModalBody>
  );
};
