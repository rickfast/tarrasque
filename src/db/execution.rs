#[derive(Debug)]
pub enum Op {
    TableScan {
        table_name: String,
    },
    PartitionLookup,
    PartitionScan,
    Filter,
    Projection {
        columns: Vec<String>,
    },
}

pub struct ExecutionContext {

}

#[derive(Debug)]
pub struct ExecutionNode {
    id: usize,
    operation: Op, // TABLE_SCAN, PARTITION_LOOKUP, FILTER, etc.
    children: Vec<usize>,
}

impl ExecutionNode {
    fn execute(&self, context: &ExecutionContext) {
        match self.operation {
            Op::TableScan => {

            },
            _ => unimplemented!()
        }
    }
}

// Execution Graph Structure
#[derive(Debug)]
pub struct ExecutionGraph {
    nodes: Vec<ExecutionNode>,
}

impl ExecutionGraph {
    pub fn new() -> Self {
        ExecutionGraph { nodes: Vec::new() }
    }

    pub fn add_node(&mut self, operation: Op, children: Vec<usize>) -> usize {
        let id = self.nodes.len();

        self.nodes.push(ExecutionNode {
            id,
            operation,
            children,
        });

        id
    }
}
