import { swapEvents, stopListenersForChain, startSwapListener } from "./swap-listener.js";
import { getClient, resetClient } from "../clients/viem-clients.js";
import { SUPPORTED_CHAINS } from "../config/chains.js";
import type { ChainId, DexType } from "../types/index.js";

const CHECK_INTERVAL_MS = 60_000;
const SWAP_SILENCE_THRESHOLD_MS = 60_000;
const REMOUNT_COOLDOWN_MS = 120_000;

const SUPPORTED_DEXES: DexType[] = ["uniswap-v3", "uniswap-v4", "pancakeswap-v3"];

const lastSwapAt = new Map<ChainId, number>();
const lastBlockNumber = new Map<ChainId, bigint>();
const lastRemountAt = new Map<ChainId, number>();

let timer: ReturnType<typeof setInterval> | null = null;
const remountingChains = new Set<ChainId>();

function markAlive(chain: ChainId): void {
  lastSwapAt.set(chain, Date.now());
}

swapEvents.on("swap", (payload: { chain: ChainId }) => {
  if (payload?.chain) markAlive(payload.chain);
});

async function remountChain(chain: ChainId): Promise<void> {
  if (remountingChains.has(chain)) return;
  remountingChains.add(chain);
  try {
    console.warn(`[LivenessProbe] [${chain}] Remounting listeners...`);
    stopListenersForChain(chain);
    resetClient(chain);
    for (const dex of SUPPORTED_DEXES) {
      try {
        await startSwapListener(chain, dex);
      } catch (err) {
        console.error(`[LivenessProbe] [${chain}] Failed to restart ${dex}:`, err);
      }
    }
    markAlive(chain);
    lastRemountAt.set(chain, Date.now());
    console.warn(`[LivenessProbe] [${chain}] Remount complete`);
  } finally {
    remountingChains.delete(chain);
  }
}

async function checkChain(chain: ChainId): Promise<void> {
  const last = lastSwapAt.get(chain) ?? Date.now();
  const silence = Date.now() - last;
  if (silence < SWAP_SILENCE_THRESHOLD_MS) return;

  let block: bigint;
  try {
    const client = getClient(chain);
    block = await client.getBlockNumber();
  } catch (err) {
    console.warn(
      `[LivenessProbe] [${chain}] getBlockNumber failed:`,
      err instanceof Error ? err.message : err
    );
    return;
  }

  const prev = lastBlockNumber.get(chain);
  lastBlockNumber.set(chain, block);

  if (prev === undefined) return;
  if (block <= prev) return;

  const lastRemount = lastRemountAt.get(chain) ?? 0;
  if (Date.now() - lastRemount < REMOUNT_COOLDOWN_MS) {
    console.warn(
      `[LivenessProbe] [${chain}] swap silence ${(silence / 1000).toFixed(0)}s but chain advances (${prev} -> ${block}); within cooldown, skip`
    );
    return;
  }

  console.warn(
    `[LivenessProbe] [${chain}] swap silence ${(silence / 1000).toFixed(0)}s but chain advances (${prev} -> ${block}); triggering remount`
  );
  await remountChain(chain);
}

export function startLivenessProbe(): void {
  if (timer) return;
  for (const chain of SUPPORTED_CHAINS) markAlive(chain);

  timer = setInterval(() => {
    for (const chain of SUPPORTED_CHAINS) {
      checkChain(chain).catch((err) => {
        console.error(`[LivenessProbe] [${chain}] check error:`, err);
      });
    }
  }, CHECK_INTERVAL_MS);
  if (typeof timer.unref === "function") timer.unref();
  console.log(
    `[LivenessProbe] Started (check every ${CHECK_INTERVAL_MS / 1000}s, silence threshold ${SWAP_SILENCE_THRESHOLD_MS / 1000}s)`
  );
}

export function stopLivenessProbe(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}
