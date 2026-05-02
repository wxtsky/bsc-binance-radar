import crypto from "crypto";
import type { AnomalyTrigger, AnomalyRule } from "../anomaly/rules.js";

export interface FeishuConfig {
  webhook: string;
  /** 启用签名校验时填，与机器人创建时设置的 secret 一致 */
  secret?: string;
}

function genSign(secret: string, timestamp: number): string {
  // 飞书签名规则：HMAC-SHA256(stringToSign, "")，stringToSign = `${timestamp}\n${secret}`
  const stringToSign = `${timestamp}\n${secret}`;
  const hmac = crypto.createHmac("sha256", stringToSign);
  hmac.update("");
  return hmac.digest("base64");
}

function ruleEmoji(rule: AnomalyRule): string {
  switch (rule) {
    case "combo":
      return "🔥";
    case "vol_spike":
      return "📈";
    case "fee_tvl_apr":
      return "💰";
    case "new_pool":
      return "🆕";
  }
}

function ruleTitle(rule: AnomalyRule): string {
  switch (rule) {
    case "combo":
      return "异动强信号 (vol + fee/TVL)";
    case "vol_spike":
      return "成交量异动";
    case "fee_tvl_apr":
      return "fee/TVL APR 高位";
    case "new_pool":
      return "新池子诞生";
  }
}

function ruleColor(rule: AnomalyRule): string {
  switch (rule) {
    case "combo":
      return "red";
    case "new_pool":
      return "blue";
    case "vol_spike":
      return "orange";
    case "fee_tvl_apr":
      return "purple";
  }
}

function fmtUsd(v?: number): string {
  if (v === undefined || !Number.isFinite(v)) return "—";
  if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`;
  if (v >= 1_000) return `$${(v / 1_000).toFixed(2)}K`;
  return `$${v.toFixed(2)}`;
}

function fmtPct(v?: number, digits = 1): string {
  if (v === undefined || !Number.isFinite(v)) return "—";
  return `${v.toFixed(digits)}%`;
}

function buildCard(t: AnomalyTrigger) {
  const m = t.metrics;
  const fields: Array<{ tag: string; text: { tag: string; content: string } }> = [];

  if (m.volRatio !== undefined && m.vol5minUsd !== undefined) {
    fields.push({
      tag: "div",
      text: {
        tag: "lark_md",
        content: `**5min Vol** ${fmtUsd(m.vol5minUsd)}　**比 24h 均值** ${m.volRatio.toFixed(1)}×`,
      },
    });
  }
  if (m.feeAprPct !== undefined && m.tvlUsd !== undefined) {
    fields.push({
      tag: "div",
      text: {
        tag: "lark_md",
        content: `**Fee/TVL 年化** ${fmtPct(m.feeAprPct, 0)}　**TVL** ${fmtUsd(m.tvlUsd)}　**5min Fee** ${fmtUsd(m.fee5minUsd)}`,
      },
    });
  }
  if (m.newPoolAddress) {
    fields.push({
      tag: "div",
      text: {
        tag: "lark_md",
        content:
          `**新池** ${m.newPoolDex} · fee=${(m.newPoolFeeTier ?? 0) / 10000}%\n` +
          `[${m.newPoolAddress.slice(0, 10)}…](https://bscscan.com/address/${m.newPoolAddress})`,
      },
    });
  }

  return {
    msg_type: "interactive",
    card: {
      header: {
        title: {
          tag: "plain_text",
          content: `${ruleEmoji(t.rule)} ${ruleTitle(t.rule)} · ${t.symbol}`,
        },
        template: ruleColor(t.rule),
      },
      elements: [
        ...fields,
        { tag: "hr" },
        {
          tag: "div",
          text: {
            tag: "lark_md",
            content:
              `🔗 [BscScan](https://bscscan.com/token/${t.tokenAddress}) · ` +
              `[DexScreener](https://dexscreener.com/bsc/${t.tokenAddress})`,
          },
        },
        {
          tag: "note",
          elements: [
            {
              tag: "plain_text",
              content: `${new Date(t.detectedAt).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" })} · ${t.tokenAddress}`,
            },
          ],
        },
      ],
    },
  };
}

export async function sendFeishuAlert(
  trigger: AnomalyTrigger,
  config: FeishuConfig
): Promise<void> {
  const card = buildCard(trigger);
  let payload: Record<string, unknown> = card as unknown as Record<string, unknown>;

  if (config.secret) {
    const timestamp = Math.floor(Date.now() / 1000);
    payload = {
      timestamp: timestamp.toString(),
      sign: genSign(config.secret, timestamp),
      ...payload,
    };
  }

  const res = await fetch(config.webhook, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Feishu webhook HTTP ${res.status}: ${body.slice(0, 200)}`);
  }
  const data = (await res.json()) as { code?: number; msg?: string };
  // 飞书 API 成功 code=0；签名错/格式错会返回 code != 0
  if (typeof data.code === "number" && data.code !== 0) {
    throw new Error(`Feishu API code=${data.code} msg=${data.msg}`);
  }
}
