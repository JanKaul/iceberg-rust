/*!
 * Defines the [MaterializedView] struct that represents an iceberg materialized view.
*/

use crate::{table::Table, view::View};

/// An iceberg materialized view
pub struct MaterializedView {
    view: View,
    storage_table: Table,
}
