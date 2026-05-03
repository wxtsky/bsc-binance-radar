use alloy::primitives::{address, Address};
use std::collections::HashSet;

/// BSC base tokens
pub const WBNB: Address = address!("bb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c");
pub const USDT: Address = address!("55d398326f99059ff775485246999027b3197955");
pub const USDC: Address = address!("8ac76a51cc950d9822d68b83fe1ad97b32cd580d");

/// Base tokens：监控范围严格限定为 (target token, base token) 配对的池子。
/// base = {WBNB, USDT, USDC}（不含 USD1 / native zero）。
pub fn bsc_base_tokens() -> HashSet<Address> {
    let mut s = HashSet::new();
    s.insert(WBNB);
    s.insert(USDT);
    s.insert(USDC);
    s
}

/// 池子收录规则：恰好一个 target token + 一个 base token（任意顺序）
pub fn pool_includes_pair(t0: Address, t1: Address, targets: &HashSet<Address>, bases: &HashSet<Address>) -> bool {
    let t0_target = targets.contains(&t0);
    let t1_target = targets.contains(&t1);
    let t0_base = bases.contains(&t0);
    let t1_base = bases.contains(&t1);
    (t0_target && t1_base) || (t1_target && t0_base)
}

/// BSC 稳定币
pub fn bsc_stable_coins() -> HashSet<Address> {
    let mut s = HashSet::new();
    s.insert(USDT);
    s.insert(USDC);
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;

    #[test]
    fn pair_filter_works_both_orders() {
        let lab = address!("1111111111111111111111111111111111111111");
        let mut targets = HashSet::new();
        targets.insert(lab);
        let bases = bsc_base_tokens();

        // target 在 t0
        assert!(pool_includes_pair(lab, USDT, &targets, &bases));
        // target 在 t1
        assert!(pool_includes_pair(USDT, lab, &targets, &bases));
        // base/base 不收
        assert!(!pool_includes_pair(USDT, WBNB, &targets, &bases));
        // 非 base 也不收
        let other = address!("2222222222222222222222222222222222222222");
        assert!(!pool_includes_pair(lab, other, &targets, &bases));
        // 非 target 也不收
        let foo = address!("3333333333333333333333333333333333333333");
        assert!(!pool_includes_pair(foo, USDT, &targets, &bases));
    }
}
