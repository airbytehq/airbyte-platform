import { ACTIONS } from "./constants";
import { Impact } from "../instatus/types";

export interface UserEvent {
  action: (typeof ACTIONS)[keyof typeof ACTIONS];
  properties: { id: string; name: string } | undefined;
  category: string;
}

export interface CurrentStatusStyles {
  color: string;
  background: string;
}
export type CurrentStatus =
  | ({
      status: "HASISSUES";
      impact: Impact;
      url: string;
      message: string;
    } & { styles: CurrentStatusStyles })
  | ({
      status: "UNDERMAINTENANCE";
      url: string;
      message: string;
    } & { styles: CurrentStatusStyles })
  | {
      status: "UP";
    };
