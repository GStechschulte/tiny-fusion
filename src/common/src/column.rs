/// A named reference to a qualified field in a schema.
pub struct Column {
    pub relation: Option<TableReference>,
    pub name: String,
    pub spans: Spans,
}
