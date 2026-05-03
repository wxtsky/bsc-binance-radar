// 事件签名分 mod 隔离，让 sol! 计算 SIGNATURE_HASH 用真实 event name（不带 Rust 前缀）

pub mod factory {
    use alloy::sol;
    sol! {
        /// V3 / PCS V3 Factory PoolCreated（同名同 sig 共用）
        #[allow(missing_docs)]
        event PoolCreated(
            address indexed token0,
            address indexed token1,
            uint24 indexed fee,
            int24 tickSpacing,
            address pool
        );
    }
}

pub mod uniswap_v4 {
    use alloy::sol;
    sol! {
        /// UniswapV4 PoolManager Initialize
        /// 顺序：id, currency0, currency1, fee, tickSpacing, hooks, sqrtPriceX96, tick
        #[allow(missing_docs)]
        event Initialize(
            bytes32 indexed id,
            address indexed currency0,
            address indexed currency1,
            uint24 fee,
            int24 tickSpacing,
            address hooks,
            uint160 sqrtPriceX96,
            int24 tick
        );

        /// UniswapV4 PoolManager Swap
        #[allow(missing_docs)]
        event Swap(
            bytes32 indexed id,
            address indexed sender,
            int128 amount0,
            int128 amount1,
            uint160 sqrtPriceX96,
            uint128 liquidity,
            int24 tick,
            uint24 fee
        );
    }
}

pub mod pancakeswap_v4_cl {
    use alloy::sol;
    sol! {
        /// PCS V4 CL PoolManager Initialize（参数顺序不同，多 bytes32 parameters）
        #[allow(missing_docs)]
        event Initialize(
            bytes32 indexed id,
            address indexed currency0,
            address indexed currency1,
            address hooks,
            uint24 fee,
            bytes32 parameters,
            uint160 sqrtPriceX96,
            int24 tick
        );

        /// PCS V4 CL PoolManager Swap（多 protocolFee）
        #[allow(missing_docs)]
        event Swap(
            bytes32 indexed id,
            address indexed sender,
            int128 amount0,
            int128 amount1,
            uint160 sqrtPriceX96,
            uint128 liquidity,
            int24 tick,
            uint24 fee,
            uint16 protocolFee
        );
    }
}

pub mod uniswap_v3 {
    use alloy::sol;
    sol! {
        /// UniswapV3 Pool Swap
        #[allow(missing_docs)]
        event Swap(
            address indexed sender,
            address indexed recipient,
            int256 amount0,
            int256 amount1,
            uint160 sqrtPriceX96,
            uint128 liquidity,
            int24 tick
        );
    }
}

pub mod pancakeswap_v3 {
    use alloy::sol;
    sol! {
        /// PCS V3 Pool Swap（多 protocolFees）
        #[allow(missing_docs)]
        event Swap(
            address indexed sender,
            address indexed recipient,
            int256 amount0,
            int256 amount1,
            uint160 sqrtPriceX96,
            uint128 liquidity,
            int24 tick,
            uint128 protocolFeesToken0,
            uint128 protocolFeesToken1
        );
    }
}

pub mod pancakeswap_v2 {
    use alloy::sol;
    sol! {
        /// V2 Pair Swap（用于 BNB price pool 反算）
        #[allow(missing_docs)]
        event Swap(
            address indexed sender,
            uint256 amount0In,
            uint256 amount1In,
            uint256 amount0Out,
            uint256 amount1Out,
            address indexed to
        );
    }
}

// 兼容旧的 alias，让其他模块直接 import
pub use factory::PoolCreated;
pub use uniswap_v4::Initialize as V4Initialize;
pub use uniswap_v4::Swap as V4Swap;
pub use pancakeswap_v4_cl::Initialize as PcsV4ClInitialize;
pub use pancakeswap_v4_cl::Swap as PcsV4ClSwap;
pub use uniswap_v3::Swap as V3Swap;
pub use pancakeswap_v3::Swap as PancakeV3Swap;
pub use pancakeswap_v2::Swap as V2Swap;
