/*!
 * Materialized view operations
*/

use anyhow::Result;

use crate::{table::transaction::TableTransaction, view::transaction::ViewTransaction};

/// View operation
pub enum Operation {
    /// Update the table location
    UpdateLocation(String),
}

impl<'mv> Operation {
    /// Execute operation
    pub async fn execute(
        self,
        view_transaction: &mut ViewTransaction<'mv>,
        table_transaction: &mut TableTransaction<'mv>,
    ) -> Result<()> {
        match self {
            Operation::UpdateLocation(location) => {
                view_transaction.update_location_mut(&location);
                Ok(())
            }
        }
    }
}
