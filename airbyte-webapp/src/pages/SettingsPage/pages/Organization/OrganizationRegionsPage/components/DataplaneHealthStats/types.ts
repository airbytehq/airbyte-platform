import { StatusCardProps } from "./components/StatusCard";

export type CardData = Pick<StatusCardProps, "level" | "value">;

export interface AgeMessage {
  messageId: string;
  values: Record<string, number>;
}
