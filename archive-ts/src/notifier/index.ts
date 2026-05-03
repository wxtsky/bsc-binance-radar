import { anomalyEvents, type AnomalyEventPayload } from "../anomaly/events.js";
import { sendFeishuAlert, type FeishuConfig } from "./feishu.js";

let registered = false;
let listener: ((payload: AnomalyEventPayload) => void) | null = null;

export function startNotifier(): void {
  if (registered) return;

  const webhook = process.env.NOTIFY_FEISHU_WEBHOOK;
  if (!webhook) {
    console.log("[Notifier] NOTIFY_FEISHU_WEBHOOK not set — anomaly alerts disabled");
    return;
  }

  const secret = process.env.NOTIFY_FEISHU_SECRET || undefined;
  const config: FeishuConfig = { webhook, secret };

  console.log(`[Notifier] Feishu enabled${secret ? " (signed)" : ""}`);

  listener = (payload: AnomalyEventPayload) => {
    sendFeishuAlert(payload.trigger, config).catch((err) => {
      console.error("[Notifier] Feishu send failed:", err instanceof Error ? err.message : err);
    });
  };
  anomalyEvents.on("anomaly", listener);
  registered = true;
}

export function stopNotifier(): void {
  if (listener) {
    anomalyEvents.off("anomaly", listener);
    listener = null;
  }
  registered = false;
}
