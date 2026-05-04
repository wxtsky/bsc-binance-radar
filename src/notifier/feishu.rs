//! 飞书 webhook 富文本卡片通知

use crate::anomaly::rules::{AnomalyRule, AnomalyTrigger};
use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use serde_json::json;

pub struct FeishuNotifier {
    pub webhook: String,
    pub secret: Option<String>,
    client: reqwest::Client,
}

impl FeishuNotifier {
    pub fn new(webhook: String, secret: Option<String>) -> Self {
        Self {
            webhook,
            secret,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
        }
    }

    pub async fn send(&self, trigger: &AnomalyTrigger) -> Result<()> {
        let dt = Utc.timestamp_millis_opt(trigger.detected_at).unwrap();
        let title = match trigger.rule {
            AnomalyRule::VolSpike => "vol_spike",
            AnomalyRule::Combo => "combo",
        };
        let template = match trigger.rule {
            AnomalyRule::VolSpike => "orange",
            AnomalyRule::Combo => "red",
        };

        let metrics_str = serde_json::to_string_pretty(&trigger.metrics)?;

        let card = json!({
            "msg_type": "interactive",
            "card": {
                "header": {
                    "template": template,
                    "title": {
                        "tag": "plain_text",
                        "content": format!("BSC 异动 [{}] {}", title, trigger.symbol)
                    }
                },
                "elements": [
                    {
                        "tag": "div",
                        "fields": [
                            { "is_short": true, "text": { "tag": "lark_md", "content": format!("**Token**：`{}`", trigger.symbol) } },
                            { "is_short": true, "text": { "tag": "lark_md", "content": format!("**时间**：{}", dt.format("%Y-%m-%d %H:%M:%S UTC")) } },
                            { "is_short": false, "text": { "tag": "lark_md", "content": format!("**地址**：`0x{}`", hex::encode(&trigger.token_address)) } },
                        ]
                    },
                    {
                        "tag": "hr"
                    },
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": format!("```json\n{}\n```", metrics_str)
                        }
                    }
                ]
            }
        });

        let resp = self.client
            .post(&self.webhook)
            .json(&card)
            .send()
            .await
            .context("feishu post failed")?;
        if !resp.status().is_success() {
            anyhow::bail!("feishu HTTP {}", resp.status());
        }
        Ok(())
    }
}
