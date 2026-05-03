use std::collections::HashSet;

/// BSC WBNB
pub const WBNB_ADDRESS: &str = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c";
/// BSC USDT
pub const USDT_ADDRESS: &str = "0x55d398326f99059ff775485246999027b3197955";
/// BSC USDC
pub const USDC_ADDRESS: &str = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d";

/// Base tokens：监控范围严格限定为 (target token, base token) 配对的池子。
/// base = {WBNB, USDT, USDC}（不含 USD1 / native zero）。
/// target = 303 个币安永续白名单 token。
/// 收录条件：恰好一个 token 是 target、另一个是 base（顺序无关）。
pub fn bsc_base_tokens() -> HashSet<String> {
    let mut s = HashSet::new();
    s.insert(WBNB_ADDRESS.to_string());
    s.insert(USDT_ADDRESS.to_string());
    s.insert(USDC_ADDRESS.to_string());
    s
}

/// 池子收录规则：恰好一个 target token + 一个 base token（任意顺序）
pub fn pool_includes_pair(t0: &str, t1: &str, targets: &HashSet<String>, bases: &HashSet<String>) -> bool {
    let t0_target = targets.contains(t0);
    let t1_target = targets.contains(t1);
    let t0_base = bases.contains(t0);
    let t1_base = bases.contains(t1);
    (t0_target && t1_base) || (t1_target && t0_base)
}

/// BSC 稳定币（用于 USD volume 计算 anchor）
pub fn bsc_stable_coins() -> HashSet<String> {
    let mut s = HashSet::new();
    s.insert(USDT_ADDRESS.to_string());
    s.insert(USDC_ADDRESS.to_string());
    s
}

pub fn bsc_price_anchor() -> String {
    WBNB_ADDRESS.to_string()
}

#[derive(Debug, Clone, Copy)]
pub struct ChainFilters {
    pub min_fee_tier: u32,
    pub max_single_swap_fee_usd: f64,
    pub pool_tvl_max_usd: f64,
}

pub const BSC_FILTERS: ChainFilters = ChainFilters {
    min_fee_tier: 0,
    max_single_swap_fee_usd: 10_000_000.0,
    pool_tvl_max_usd: 100_000_000.0,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pair_filter_works_both_orders() {
        let mut targets = HashSet::new();
        targets.insert("0xlab".to_string());
        let bases = bsc_base_tokens();

        // target 在 t0
        assert!(pool_includes_pair("0xlab", USDT_ADDRESS, &targets, &bases));
        // target 在 t1
        assert!(pool_includes_pair(USDT_ADDRESS, "0xlab", &targets, &bases));
        // base/base 不收
        assert!(!pool_includes_pair(USDT_ADDRESS, WBNB_ADDRESS, &targets, &bases));
        // 非 base 也不收
        assert!(!pool_includes_pair("0xlab", "0xother", &targets, &bases));
        // 非 target 也不收
        assert!(!pool_includes_pair("0xfoo", USDT_ADDRESS, &targets, &bases));
    }
}
