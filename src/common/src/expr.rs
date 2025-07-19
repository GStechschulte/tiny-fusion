/// Represents logical expressions such as `A + 1`
pub enum Expr {
    /// A named reference to a qualified field in a schema.
    Column(Column),
    /// A constant value.
    Literal(ScalarValue),
    /// A binary expression such as "age > 21".
    BinaryExpr(BinaryExpr),
}
