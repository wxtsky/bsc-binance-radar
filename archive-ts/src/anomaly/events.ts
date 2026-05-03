import { EventEmitter } from "events";
import type { AnomalyTrigger } from "./rules.js";

/**
 * 异动检测器触发时 emit "anomaly" 事件。
 * Phase 4 notifier 订阅此事件做 Telegram / webhook 推送。
 */
export const anomalyEvents = new EventEmitter();
anomalyEvents.setMaxListeners(50);

export interface AnomalyEventPayload {
  trigger: AnomalyTrigger;
}
