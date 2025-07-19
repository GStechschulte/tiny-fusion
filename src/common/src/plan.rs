use std::sync::Arc;

use crate::expr::Expr;

/// A `LogicalPlan` is a node in a tree of relational operators (such as
/// Projection or Filter).
pub enum LogicalPlan {
    Scan(Scan),
    /// Evaluates an arbitrary list of expressions
    Projection(Projection),
    Filter(Filter),
    Aggregate(Aggregate),
}

/// Projection logical plan applies a projection to its input. A projection
/// is a list of expressions to be evaluated against the input data.
pub struct Projection {
    /// The vector of expressions
    pub expr: Vec<Expr>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The schema description of the output
    pub schema: DFSchemaRef,
}
