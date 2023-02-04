use core::fmt;
use std::{collections::HashMap, rc::Rc};

use crate::{
    aggregators::{Aggregator, AvgAgg, SumAgg},
    data_types::DataType,
    expressions::Expression,
    DBError, DBResult,
};

pub trait Function: std::fmt::Debug {
    fn is_aggregator(&self) -> bool;

    fn aggregator(&self) -> DBResult<Box<dyn Aggregator>>;

    fn to_expr_string(&self) -> String {
        let args = self
            .args()
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        format!("{}({})", self.name(), args)
    }

    fn name(&self) -> String;

    fn args(&self) -> Vec<&Expression>;

    fn data_type(&self) -> DataType;

    fn with_args(&mut self, args: Vec<Expression>) -> DBResult<()>;
}

#[derive(Debug)]
pub struct FunctionsRegistry {
    functions: HashMap<String, FunctionBuilder>,
}

impl Default for FunctionsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionsRegistry {
    pub fn new() -> Self {
        let mut functions = HashMap::new();
        functions.insert("sum".to_string(), FunctionBuilder::new(SumFunction::build));
        functions.insert("avg".to_string(), FunctionBuilder::new(AvgFunction::build));

        Self { functions }
    }
    pub fn get_function(
        &self,
        name: &str,
        args: &[Expression],
    ) -> DBResult<Option<Rc<dyn Function>>> {
        match self.functions.get(name) {
            Some(fn_builder) => fn_builder.build(args).map(Some),
            None => Ok(None),
        }
    }
}

pub struct FunctionBuilder {
    builder: fn(&[Expression]) -> DBResult<Rc<dyn Function>>,
}

impl fmt::Debug for FunctionBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FunctionBuilder").finish()
    }
}

impl FunctionBuilder {
    pub fn new(builder: fn(&[Expression]) -> DBResult<Rc<dyn Function>>) -> Self {
        Self { builder }
    }
    pub fn build(&self, args: &[Expression]) -> DBResult<Rc<dyn Function>> {
        (self.builder)(args)
    }
}

#[derive(Debug, Clone)]
pub struct SumFunction {
    arg: Expression,
}

impl SumFunction {
    fn build(args: &[Expression]) -> DBResult<Rc<dyn Function>> {
        if args.len() == 1 {
            Ok(Rc::new(SumFunction {
                arg: args[0].clone(),
            }) as Rc<dyn Function>)
        } else {
            Err(DBError::Unknown("invalid args".to_string()))
        }
    }
}

impl Function for SumFunction {
    fn is_aggregator(&self) -> bool {
        true
    }

    fn aggregator(&self) -> DBResult<Box<dyn Aggregator>> {
        SumAgg::new(&self.arg.clone()).map(|agg| Box::new(agg) as Box<dyn Aggregator>)
    }

    fn name(&self) -> String {
        "sum".to_string()
    }

    fn args(&self) -> Vec<&Expression> {
        vec![&self.arg]
    }

    fn data_type(&self) -> DataType {
        self.arg.data_type()
    }

    fn with_args(&mut self, args: Vec<Expression>) -> DBResult<()> {
        if args.len() == 1 {
            self.arg = args.into_iter().next().unwrap();
            Ok(())
        } else {
            Err(DBError::Unknown("invalid args".to_string()))
        }
    }
}

#[derive(Debug, Clone)]
pub struct AvgFunction {
    arg: Expression,
}

impl AvgFunction {
    fn build(args: &[Expression]) -> DBResult<Rc<dyn Function>> {
        if args.len() == 1 {
            Ok(Rc::new(AvgFunction {
                arg: args[0].clone(),
            }) as Rc<dyn Function>)
        } else {
            Err(DBError::Unknown("invalid args".to_string()))
        }
    }
}

impl Function for AvgFunction {
    fn is_aggregator(&self) -> bool {
        true
    }

    fn aggregator(&self) -> DBResult<Box<dyn Aggregator>> {
        AvgAgg::new(&self.arg.clone()).map(|agg| Box::new(agg) as Box<dyn Aggregator>)
    }

    fn name(&self) -> String {
        "avg".to_string()
    }

    fn args(&self) -> Vec<&Expression> {
        vec![&self.arg]
    }

    fn data_type(&self) -> DataType {
        DataType::Float64
    }

    fn with_args(&mut self, args: Vec<Expression>) -> DBResult<()> {
        if args.len() == 1 {
            self.arg = args.into_iter().next().unwrap();
            Ok(())
        } else {
            Err(DBError::Unknown("invalid args".to_string()))
        }
    }
}
