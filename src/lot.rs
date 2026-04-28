use std::sync::Arc;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::Date;

use crate::id::{AccountId, LotId, SecurityId};

/// A single tax lot — one purchase of a security in one account.
///
/// Quantity is mutable only in the sense that the engine consumes lots by
/// producing [`SaleAllocation`]s; the [`LotData`] values themselves are stored
/// in an immutable [`Arc`] slice on the account.
#[derive(Clone, Debug)]
pub struct LotData {
    pub id: LotId,
    /// Caller-supplied external identifier (e.g. brokerage lot tag). Empty
    /// when the lot was synthesised from a bare share count.
    pub external_id: Arc<str>,
    pub account: AccountId,
    pub security: SecurityId,
    pub quantity: i64,
    pub cost_basis_per_share: Decimal,
    pub acquired: Date,
}

impl LotData {
    pub fn total_basis(&self) -> Decimal {
        self.cost_basis_per_share * Decimal::from(self.quantity)
    }
}

/// Strategy for choosing which tax lots to sell first.
///
/// Each variant defines a total order over a position's lots; the engine sells
/// from the "front" of that order until the requested share count is met.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum LotSelector {
    /// Oldest lots first.
    #[default]
    Fifo,
    /// Newest lots first.
    Lifo,
    /// Highest cost basis per share first (minimise realised gain).
    Hifo,
    /// Lowest cost basis per share first (realise gains to use losses).
    Lofo,
}

impl LotSelector {
    /// Sort the provided lot indices so that "sell first" lots come earliest.
    ///
    /// Ties are broken by `acquired` date then `LotId` so the result is stable
    /// across runs.
    pub fn order(self, lots: &[LotData]) -> Vec<usize> {
        let mut idx: Vec<usize> = (0..lots.len()).collect();
        idx.sort_by(|&a, &b| {
            let la = &lots[a];
            let lb = &lots[b];
            let primary = match self {
                LotSelector::Fifo => la.acquired.cmp(&lb.acquired),
                LotSelector::Lifo => lb.acquired.cmp(&la.acquired),
                LotSelector::Hifo => lb.cost_basis_per_share.cmp(&la.cost_basis_per_share),
                LotSelector::Lofo => la.cost_basis_per_share.cmp(&lb.cost_basis_per_share),
            };
            primary
                .then_with(|| la.acquired.cmp(&lb.acquired))
                .then_with(|| la.id.cmp(&lb.id))
        });
        idx
    }
}

/// A slice of a lot consumed to satisfy a sell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SaleAllocation {
    pub lot_id: LotId,
    pub external_id: Arc<str>,
    pub shares_sold: i64,
    pub price: Decimal,
    pub cost_basis_per_share: Decimal,
    pub acquired: Date,
    pub holding_days: i64,
    pub is_long_term: bool,
}

impl SaleAllocation {
    pub fn proceeds(&self) -> Decimal {
        self.price * Decimal::from(self.shares_sold)
    }

    pub fn total_basis(&self) -> Decimal {
        self.cost_basis_per_share * Decimal::from(self.shares_sold)
    }

    pub fn realized_gain(&self) -> Decimal {
        self.proceeds() - self.total_basis()
    }
}

/// Consume `shares_to_sell` shares from `lots` in `selector` order.
///
/// Returns the per-lot sale allocations and leaves the mutable `remaining`
/// vector reflecting post-sale lot quantities. The caller is responsible for
/// ensuring `shares_to_sell <= sum(remaining)`.
pub fn consume_lots(
    selector: LotSelector,
    lots: &[LotData],
    remaining: &mut [i64],
    price: Decimal,
    as_of: Date,
    mut shares_to_sell: i64,
) -> Vec<SaleAllocation> {
    if shares_to_sell <= 0 {
        return Vec::new();
    }
    let order = selector.order(lots);
    let mut out = Vec::new();
    for i in order {
        if shares_to_sell == 0 {
            break;
        }
        let take = remaining[i].min(shares_to_sell);
        if take == 0 {
            continue;
        }
        remaining[i] -= take;
        shares_to_sell -= take;
        let lot = &lots[i];
        let holding_days = (as_of - lot.acquired).whole_days();
        out.push(SaleAllocation {
            lot_id: lot.id,
            external_id: Arc::clone(&lot.external_id),
            shares_sold: take,
            price,
            cost_basis_per_share: lot.cost_basis_per_share,
            acquired: lot.acquired,
            holding_days,
            is_long_term: holding_days >= 365,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use rust_decimal_macros::dec;
    use time::macros::date;

    fn lot(id: u32, acquired: Date, basis: Decimal, qty: i64) -> LotData {
        LotData {
            id: LotId(id),
            external_id: Arc::<str>::from(""),
            account: AccountId(0),
            security: SecurityId(0),
            quantity: qty,
            cost_basis_per_share: basis,
            acquired,
        }
    }

    #[test]
    fn fifo_picks_oldest_first() {
        let lots = vec![
            lot(1, date!(2024 - 01 - 01), dec!(100), 10),
            lot(2, date!(2022 - 06 - 15), dec!(200), 10),
            lot(3, date!(2023 - 03 - 01), dec!(150), 10),
        ];
        let order = LotSelector::Fifo.order(&lots);
        assert_eq!(order, vec![1, 2, 0]);
    }

    #[test]
    fn lifo_picks_newest_first() {
        let lots = vec![
            lot(1, date!(2024 - 01 - 01), dec!(100), 10),
            lot(2, date!(2022 - 06 - 15), dec!(200), 10),
            lot(3, date!(2023 - 03 - 01), dec!(150), 10),
        ];
        let order = LotSelector::Lifo.order(&lots);
        assert_eq!(order, vec![0, 2, 1]);
    }

    #[test]
    fn hifo_picks_highest_basis_first() {
        let lots = vec![
            lot(1, date!(2024 - 01 - 01), dec!(100), 10),
            lot(2, date!(2022 - 06 - 15), dec!(200), 10),
            lot(3, date!(2023 - 03 - 01), dec!(150), 10),
        ];
        let order = LotSelector::Hifo.order(&lots);
        assert_eq!(order, vec![1, 2, 0]);
    }

    #[test]
    fn lofo_picks_lowest_basis_first() {
        let lots = vec![
            lot(1, date!(2024 - 01 - 01), dec!(100), 10),
            lot(2, date!(2022 - 06 - 15), dec!(200), 10),
            lot(3, date!(2023 - 03 - 01), dec!(150), 10),
        ];
        let order = LotSelector::Lofo.order(&lots);
        assert_eq!(order, vec![0, 2, 1]);
    }

    #[test]
    fn ties_break_by_acquired_then_id() {
        let lots = vec![
            lot(2, date!(2023 - 01 - 01), dec!(100), 10),
            lot(1, date!(2023 - 01 - 01), dec!(100), 10),
        ];
        let order = LotSelector::Hifo.order(&lots);
        assert_eq!(order, vec![1, 0]);
    }

    #[test]
    fn consume_splits_across_lots_in_order() {
        let lots = vec![
            lot(1, date!(2024 - 01 - 01), dec!(100), 10),
            lot(2, date!(2022 - 06 - 15), dec!(200), 10),
        ];
        let mut remaining = vec![10, 10];
        let sales = consume_lots(
            LotSelector::Fifo,
            &lots,
            &mut remaining,
            dec!(300),
            date!(2025 - 01 - 01),
            15,
        );
        assert_eq!(sales.len(), 2);
        assert_eq!(sales[0].lot_id, LotId(2));
        assert_eq!(sales[0].shares_sold, 10);
        assert_eq!(sales[1].lot_id, LotId(1));
        assert_eq!(sales[1].shares_sold, 5);
        assert_eq!(remaining, vec![5, 0]);
    }

    #[test]
    fn consume_marks_long_term_based_on_holding_period() {
        let lots = vec![lot(1, date!(2023 - 01 - 01), dec!(100), 10)];
        let mut remaining = vec![10];
        let sales = consume_lots(
            LotSelector::Fifo,
            &lots,
            &mut remaining,
            dec!(150),
            date!(2024 - 01 - 01),
            5,
        );
        assert!(sales[0].is_long_term);
        assert_eq!(sales[0].holding_days, 365);

        let mut remaining = vec![10];
        let sales = consume_lots(
            LotSelector::Fifo,
            &lots,
            &mut remaining,
            dec!(150),
            date!(2023 - 06 - 01),
            5,
        );
        assert!(!sales[0].is_long_term);
    }

    #[test]
    fn realized_gain_math() {
        let sale = SaleAllocation {
            lot_id: LotId(1),
            external_id: Arc::<str>::from(""),
            shares_sold: 5,
            price: dec!(150),
            cost_basis_per_share: dec!(100),
            acquired: date!(2023 - 01 - 01),
            holding_days: 500,
            is_long_term: true,
        };
        assert_eq!(sale.proceeds(), dec!(750));
        assert_eq!(sale.total_basis(), dec!(500));
        assert_eq!(sale.realized_gain(), dec!(250));
    }

    #[test]
    fn consume_zero_shares_is_noop() {
        let lots = vec![lot(1, date!(2023 - 01 - 01), dec!(100), 10)];
        let mut remaining = vec![10];
        let sales = consume_lots(
            LotSelector::Fifo,
            &lots,
            &mut remaining,
            dec!(150),
            date!(2024 - 01 - 01),
            0,
        );
        assert!(sales.is_empty());
        assert_eq!(remaining, vec![10]);
    }
}
