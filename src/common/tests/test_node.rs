use std::fmt;
use std::sync::Arc;

// Result type for transformations
#[derive(Debug, Clone)]
pub enum Transformed<T> {
    Yes(T), // Node was transformed
    No(T),  // Node was not transformed
}

impl<T> Transformed<T> {
    pub fn into_inner(self) -> T {
        match self {
            Transformed::Yes(t) | Transformed::No(t) => t,
        }
    }

    pub fn was_transformed(&self) -> bool {
        matches!(self, Transformed::Yes(_))
    }
}

// TreeNode trait - core abstraction for tree traversal and transformation
pub trait TreeNode: Sized {
    /// Apply a function to all children of this node
    fn apply_children<F>(&self, f: F) -> Result<Transformed<Self>, String>
    where
        F: Fn(&Self) -> Result<Transformed<Self>, String>;

    /// Transform this node by applying a function to all its children first
    fn map_children<F>(self, f: F) -> Result<Transformed<Self>, String>
    where
        F: Fn(Self) -> Result<Transformed<Self>, String>;

    /// Apply a transformation function to this node and all its descendants (post-order)
    fn transform<F>(&self, f: F) -> Result<Transformed<Self>, String>
    where
        F: Fn(&Self) -> Result<Transformed<Self>, String>,
    {
        // First, recursively transform all children
        let transformed_children = self.apply_children(|node| node.transform(&f))?;

        // Then apply the transformation to this node
        let node = transformed_children.into_inner();
        f(&node)
    }

    /// Apply a transformation function that can mutate the tree (consumes self)
    fn transform_down<F>(self, f: F) -> Result<Transformed<Self>, String>
    where
        F: Fn(Self) -> Result<Transformed<Self>, String>,
    {
        // Apply transformation to this node first (pre-order)
        let transformed_node = f(self)?;

        // Then recursively transform children
        let node = transformed_node.into_inner();
        node.map_children(|child| child.transform_down(&f))
    }
}

// Example logical plan nodes
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    TableScan {
        table_name: String,
        projected_columns: Vec<String>,
    },
    Filter {
        predicate: Expression,
        input: Arc<LogicalPlan>,
    },
    Projection {
        expressions: Vec<Expression>,
        input: Arc<LogicalPlan>,
    },
    Join {
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        join_type: JoinType,
        on: Vec<(String, String)>,
    },
    Limit {
        limit: usize,
        input: Arc<LogicalPlan>,
    },
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub enum Expression {
    Column(String),
    Literal(i64),
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    IsNull(Box<Expression>),
}

#[derive(Debug, Clone)]
pub enum BinaryOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Plus,
    Minus,
}

// Implement TreeNode for LogicalPlan
impl TreeNode for LogicalPlan {
    fn apply_children<F>(&self, f: F) -> Result<Transformed<Self>, String>
    where
        F: Fn(&Self) -> Result<Transformed<Self>, String>,
    {
        match self {
            LogicalPlan::TableScan { .. } => {
                // Leaf node - no children to transform
                Ok(Transformed::No(self.clone()))
            }
            LogicalPlan::Filter { predicate, input } => {
                let transformed_input = f(input)?;
                if transformed_input.was_transformed() {
                    Ok(Transformed::Yes(LogicalPlan::Filter {
                        predicate: predicate.clone(),
                        input: Arc::new(transformed_input.into_inner()),
                    }))
                } else {
                    Ok(Transformed::No(self.clone()))
                }
            }
            LogicalPlan::Projection { expressions, input } => {
                let transformed_input = f(input)?;
                if transformed_input.was_transformed() {
                    Ok(Transformed::Yes(LogicalPlan::Projection {
                        expressions: expressions.clone(),
                        input: Arc::new(transformed_input.into_inner()),
                    }))
                } else {
                    Ok(Transformed::No(self.clone()))
                }
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                on,
            } => {
                let transformed_left = f(left)?;
                let transformed_right = f(right)?;

                if transformed_left.was_transformed() || transformed_right.was_transformed() {
                    Ok(Transformed::Yes(LogicalPlan::Join {
                        left: Arc::new(transformed_left.into_inner()),
                        right: Arc::new(transformed_right.into_inner()),
                        join_type: join_type.clone(),
                        on: on.clone(),
                    }))
                } else {
                    Ok(Transformed::No(self.clone()))
                }
            }
            LogicalPlan::Limit { limit, input } => {
                let transformed_input = f(input)?;
                if transformed_input.was_transformed() {
                    Ok(Transformed::Yes(LogicalPlan::Limit {
                        limit: *limit,
                        input: Arc::new(transformed_input.into_inner()),
                    }))
                } else {
                    Ok(Transformed::No(self.clone()))
                }
            }
        }
    }

    fn map_children<F>(self, f: F) -> Result<Transformed<Self>, String>
    where
        F: Fn(Self) -> Result<Transformed<Self>, String>,
    {
        match self {
            LogicalPlan::TableScan { .. } => {
                // Leaf node - no children to transform
                Ok(Transformed::No(self))
            }
            LogicalPlan::Filter { predicate, input } => {
                let input_plan = Arc::try_unwrap(input).unwrap_or_else(|arc| (*arc).clone());
                let transformed_input = f(input_plan)?;
                if transformed_input.was_transformed() {
                    Ok(Transformed::Yes(LogicalPlan::Filter {
                        predicate,
                        input: Arc::new(transformed_input.into_inner()),
                    }))
                } else {
                    Ok(Transformed::No(LogicalPlan::Filter {
                        predicate,
                        input: Arc::new(transformed_input.into_inner()),
                    }))
                }
            }
            LogicalPlan::Projection { expressions, input } => {
                let input_plan = Arc::try_unwrap(input).unwrap_or_else(|arc| (*arc).clone());
                let transformed_input = f(input_plan)?;
                if transformed_input.was_transformed() {
                    Ok(Transformed::Yes(LogicalPlan::Projection {
                        expressions,
                        input: Arc::new(transformed_input.into_inner()),
                    }))
                } else {
                    Ok(Transformed::No(LogicalPlan::Projection {
                        expressions,
                        input: Arc::new(transformed_input.into_inner()),
                    }))
                }
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                on,
            } => {
                let left_plan = Arc::try_unwrap(left).unwrap_or_else(|arc| (*arc).clone());
                let right_plan = Arc::try_unwrap(right).unwrap_or_else(|arc| (*arc).clone());

                let transformed_left = f(left_plan)?;
                let transformed_right = f(right_plan)?;

                if transformed_left.was_transformed() || transformed_right.was_transformed() {
                    Ok(Transformed::Yes(LogicalPlan::Join {
                        left: Arc::new(transformed_left.into_inner()),
                        right: Arc::new(transformed_right.into_inner()),
                        join_type,
                        on,
                    }))
                } else {
                    Ok(Transformed::No(LogicalPlan::Join {
                        left: Arc::new(transformed_left.into_inner()),
                        right: Arc::new(transformed_right.into_inner()),
                        join_type,
                        on,
                    }))
                }
            }
            LogicalPlan::Limit { limit, input } => {
                let input_plan = Arc::try_unwrap(input).unwrap_or_else(|arc| (*arc).clone());
                let transformed_input = f(input_plan)?;
                if transformed_input.was_transformed() {
                    Ok(Transformed::Yes(LogicalPlan::Limit {
                        limit,
                        input: Arc::new(transformed_input.into_inner()),
                    }))
                } else {
                    Ok(Transformed::No(LogicalPlan::Limit {
                        limit,
                        input: Arc::new(transformed_input.into_inner()),
                    }))
                }
            }
        }
    }
}

// Example optimization rules
pub struct OptimizationRule;

impl OptimizationRule {
    /// Rule: Push down limits through projections
    pub fn push_down_limit(plan: &LogicalPlan) -> Result<Transformed<LogicalPlan>, String> {
        match plan {
            LogicalPlan::Limit { limit, input } => {
                match input.as_ref() {
                    LogicalPlan::Projection {
                        expressions,
                        input: proj_input,
                    } => {
                        // Push limit below projection
                        let new_limit = LogicalPlan::Limit {
                            limit: *limit,
                            input: proj_input.clone(),
                        };
                        let new_projection = LogicalPlan::Projection {
                            expressions: expressions.clone(),
                            input: Arc::new(new_limit),
                        };
                        Ok(Transformed::Yes(new_projection))
                    }
                    _ => Ok(Transformed::No(plan.clone())),
                }
            }
            _ => Ok(Transformed::No(plan.clone())),
        }
    }

    /// Rule: Remove redundant projections
    pub fn remove_redundant_projection(
        plan: &LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>, String> {
        match plan {
            LogicalPlan::Projection { expressions, input } => {
                // Check if projection is just selecting all columns in order
                if let LogicalPlan::TableScan {
                    projected_columns, ..
                } = input.as_ref()
                {
                    let expr_columns: Vec<String> = expressions
                        .iter()
                        .filter_map(|expr| match expr {
                            Expression::Column(name) => Some(name.clone()),
                            _ => None,
                        })
                        .collect();

                    if expr_columns == *projected_columns {
                        // Redundant projection - remove it
                        return Ok(Transformed::Yes(input.as_ref().clone()));
                    }
                }
                Ok(Transformed::No(plan.clone()))
            }
            _ => Ok(Transformed::No(plan.clone())),
        }
    }

    /// Rule: Combine consecutive filters
    pub fn combine_filters(plan: &LogicalPlan) -> Result<Transformed<LogicalPlan>, String> {
        match plan {
            LogicalPlan::Filter {
                predicate: pred1,
                input,
            } => {
                if let LogicalPlan::Filter {
                    predicate: pred2,
                    input: inner_input,
                } = input.as_ref()
                {
                    // Combine two filters with AND
                    let combined_predicate = Expression::BinaryOp {
                        left: Box::new(pred1.clone()),
                        op: BinaryOperator::And,
                        right: Box::new(pred2.clone()),
                    };
                    let combined_filter = LogicalPlan::Filter {
                        predicate: combined_predicate,
                        input: inner_input.clone(),
                    };
                    Ok(Transformed::Yes(combined_filter))
                } else {
                    Ok(Transformed::No(plan.clone()))
                }
            }
            _ => Ok(Transformed::No(plan.clone())),
        }
    }
}

// Example usage and demonstration
fn main() -> Result<(), String> {
    // Create a sample logical plan
    let table_scan = LogicalPlan::TableScan {
        table_name: "employees".to_string(),
        projected_columns: vec!["id".to_string(), "name".to_string(), "salary".to_string()],
    };

    let filter1 = LogicalPlan::Filter {
        predicate: Expression::BinaryOp {
            left: Box::new(Expression::Column("salary".to_string())),
            op: BinaryOperator::Gt,
            right: Box::new(Expression::Literal(50000)),
        },
        input: Arc::new(table_scan),
    };

    let filter2 = LogicalPlan::Filter {
        predicate: Expression::BinaryOp {
            left: Box::new(Expression::Column("id".to_string())),
            op: BinaryOperator::Lt,
            right: Box::new(Expression::Literal(1000)),
        },
        input: Arc::new(filter1),
    };

    let projection = LogicalPlan::Projection {
        expressions: vec![
            Expression::Column("id".to_string()),
            Expression::Column("name".to_string()),
            Expression::Column("salary".to_string()),
        ],
        input: Arc::new(filter2),
    };

    let limit = LogicalPlan::Limit {
        limit: 10,
        input: Arc::new(projection),
    };

    println!("Original plan:");
    println!("{:#?}", limit);

    // Apply optimization rules using TreeNode trait
    println!("\n--- Applying Optimizations ---");

    // 1. Combine consecutive filters
    let optimized1 = limit.transform(OptimizationRule::combine_filters)?;
    println!("\nAfter combining filters:");
    println!("{:#?}", optimized1.into_inner());

    // 2. Push down limit through projection
    let optimized2 = optimized1
        .into_inner()
        .transform(OptimizationRule::push_down_limit)?;
    println!("\nAfter pushing down limit:");
    println!("{:#?}", optimized2.into_inner());

    // 3. Remove redundant projection
    let optimized3 = optimized2
        .into_inner()
        .transform(OptimizationRule::remove_redundant_projection)?;
    println!("\nAfter removing redundant projection:");
    println!("{:#?}", optimized3.into_inner());

    // Example of applying multiple rules in sequence
    let final_plan = limit.transform(|plan| {
        let step1 = OptimizationRule::combine_filters(plan)?;
        let step2 = OptimizationRule::push_down_limit(&step1.into_inner())?;
        let step3 = OptimizationRule::remove_redundant_projection(&step2.into_inner())?;
        Ok(step3)
    })?;

    println!("\nFinal optimized plan:");
    println!("{:#?}", final_plan.into_inner());

    Ok(())
}
