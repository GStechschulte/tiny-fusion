pub trait TreeNode: Sized {
    /// Apply a function to all children of this node.
    fn apply_children<F>(&self, f: F) -> Result<Transformed<Self>, String>
    where
        F: Fn(&Self) -> Result<Transformed<Self>, String>;
}
