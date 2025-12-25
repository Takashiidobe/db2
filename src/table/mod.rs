pub mod heap;
pub mod scan;

pub use heap::{HeapTable, RowId};
pub use scan::TableScan;

#[cfg(test)]
mod heap_test;
#[cfg(test)]
mod scan_test;
