use std::sync::Arc;

use crate::expr::Expr;

/// A `LogicalPlan` is a node in a tree of relational operators (such as
/// Projection or Filter).
pub enum LogicalPlan {
    TableScan(TableScan),
    /// Evaluates an arbitrary list of expressions
    Projection(Projection),
    Filter(Filter),
    /// Skip some number of rows, and then fetch some number of rows.
    Limit(Limit),
    /// Join two logical plans on one or more join columns.
    Join(Join),
}

pub struct TableScan {
    pub table_name: String,
    pub projected_columns: Vec<String>,
}

/// Projection logical plan applies a projection to its input. A projection
/// is a list of expressions to be evaluated against the input data.
pub struct Projection {
    /// The vector of expressions
    pub expr: Vec<Expr>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
}

pub struct Filter {
    pub predicate: Expr,
    /// The incoming logical pan
    pub input: Arc<LogicalPlan>,
}

pub struct Limit {
    /// Maximum number of rows to fetch.
    pub fetch: usize,
    pub input: Arc<LogicalPlan>,
}

pub struct Join {
    left: Arc<LogicalPlan>,
    right: Arc<LogicalPlan>,
    on: Vec<(String, String)>,
    join_type: JoinType,
}

pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}
