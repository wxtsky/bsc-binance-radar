import { createPublicClient, webSocket, type PublicClient } from "viem";
import { bsc } from "viem/chains";
import { CHAIN_CONFIGS } from "../config/chains.js";
import type { ChainId } from "../types/index.js";

const viemChains = { bsc } as const;

const WS_OPTIONS = {
  reconnect: {
    attempts: 1_000_000,
    delay: 2_000,
  },
  keepAlive: {
    interval: 10_000,
  },
  retryCount: 5,
  retryDelay: 200,
} as const;

const clients = new Map<ChainId, PublicClient>();

export function getClient(chain: ChainId): PublicClient {
  let client = clients.get(chain);
  if (!client) {
    const config = CHAIN_CONFIGS[chain];

    if (!config.wssUrl) {
      throw new Error(`[Radar] ${chain.toUpperCase()}_WSS_URL is required for swap listeners.`);
    }

    client = createPublicClient({
      chain: viemChains[chain],
      transport: webSocket(config.wssUrl, WS_OPTIONS),
      batch: { multicall: true },
    }) as PublicClient;

    clients.set(chain, client);
    console.log(`[Radar] [${chain}] WebSocket connected`);
  }
  return client!;
}

export function resetClient(chain: ChainId): void {
  clients.delete(chain);
  console.log(`[Radar] [${chain}] WebSocket client reset`);
}
