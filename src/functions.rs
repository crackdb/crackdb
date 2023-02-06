mod agg_function;

use core::fmt;
use std::{collections::HashMap, rc::Rc};

use crate::{
    aggregators::{Aggregator, AvgAgg, CountAgg, MaxAgg, MinAgg, SumAgg},
    data_types::DataType,
    expressions::Expression,
    DBError, DBResult,
};

use self::agg_function::AggFunction;

pub trait Function: std::fmt::Debug {
    /// TODO: consider remove this method
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

    /// TODO: the return signature is wired
    fn with_args(&self, args: Vec<Expression>) -> DBResult<Rc<dyn Function>>;
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
        // TODO: revisit this, it's too complex
        functions.insert(
            "sum".to_string(),
            FunctionBuilder::new_aggregator(SumFunction::build),
        );
        functions.insert(
            "avg".to_string(),
            FunctionBuilder::new_aggregator(AvgFunction::build),
        );
        functions.insert(
            "count".to_string(),
            FunctionBuilder::new_aggregator(CountFunction::build),
        );
        functions.insert(
            "max".to_string(),
            FunctionBuilder::new_aggregator(MaxFunction::build),
        );
        functions.insert(
            "min".to_string(),
            FunctionBuilder::new_aggregator(MinFunction::build),
        );
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

    pub fn is_aggregator(&self, expr: &Expression) -> bool {
        match expr {
            Expression::UnResolvedFunction { name, .. } => self
                .functions
                .get(name)
                .map(|f| f.is_aggregator)
                .unwrap_or(false),
            Expression::Function(f) => f.is_aggregator(),
            _ => false,
        }
    }
}

pub struct FunctionBuilder {
    builder: fn(&[Expression]) -> DBResult<Rc<dyn Function>>,
    is_aggregator: bool,
}

impl fmt::Debug for FunctionBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FunctionBuilder").finish()
    }
}

impl FunctionBuilder {
    pub fn _new(builder: fn(&[Expression]) -> DBResult<Rc<dyn Function>>) -> Self {
        Self {
            builder,
            is_aggregator: false,
        }
    }
    pub fn new_aggregator(
        builder: fn(&[Expression]) -> DBResult<Rc<dyn Function>>,
    ) -> Self {
        Self {
            builder,
            is_aggregator: true,
        }
    }

    pub fn build(&self, args: &[Expression]) -> DBResult<Rc<dyn Function>> {
        (self.builder)(args)
    }
}

pub struct SumFunction {}

impl SumFunction {
    fn build(args: &[Expression]) -> DBResult<Rc<dyn Function>> {
        if args.len() == 1 {
            let arg = args[0].clone();
            let function = AggFunction::new(
                "sum",
                Rc::new(|arg| arg.data_type()),
                arg,
                Rc::new(|arg| {
                    SumAgg::new(arg).map(|agg| Box::new(agg) as Box<dyn Aggregator>)
                }),
            );
            Ok(Rc::new(function) as Rc<dyn Function>)
        } else {
            Err(DBError::Unknown("invalid args".to_string()))
        }
    }
}

pub struct AvgFunction {}

impl AvgFunction {
    fn build(args: &[Expression]) -> DBResult<Rc<dyn Function>> {
        if args.len() == 1 {
            let arg = args[0].clone();
            let function = AggFunction::new(
                "avg",
                Rc::new(|_| DataType::Float64),
                arg,
                Rc::new(|arg| {
                    AvgAgg::new(arg).map(|agg| Box::new(agg) as Box<dyn Aggregator>)
                }),
            );
            Ok(Rc::new(function) as Rc<dyn Function>)
        } else {
            Err(DBError::Unknown("invalid args".to_string()))
        }
    }
}

pub struct CountFunction {}

impl CountFunction {
    fn build(args: &[Expression]) -> DBResult<Rc<dyn Function>> {
        if args.len() == 1 {
            let arg = args[0].clone();
            let function = AggFunction::new(
                "count",
                Rc::new(|_| DataType::UInt64),
                arg,
                Rc::new(|arg| {
                    CountAgg::new(arg).map(|agg| Box::new(agg) as Box<dyn Aggregator>)
                }),
            );
            Ok(Rc::new(function) as Rc<dyn Function>)
        } else {
            Err(DBError::Unknown("invalid args".to_string()))
        }
    }
}

pub struct MaxFunction {}

impl MaxFunction {
    fn build(args: &[Expression]) -> DBResult<Rc<dyn Function>> {
        if args.len() == 1 {
            let arg = args[0].clone();
            let function = AggFunction::new(
                "max",
                Rc::new(|arg| arg.data_type()),
                arg,
                Rc::new(|arg| {
                    MaxAgg::new(arg).map(|agg| Box::new(agg) as Box<dyn Aggregator>)
                }),
            );
            Ok(Rc::new(function) as Rc<dyn Function>)
        } else {
            Err(DBError::Unknown("invalid args".to_string()))
        }
    }
}

pub struct MinFunction {}

impl MinFunction {
    fn build(args: &[Expression]) -> DBResult<Rc<dyn Function>> {
        if args.len() == 1 {
            let arg = args[0].clone();
            let function = AggFunction::new(
                "min",
                Rc::new(|arg| arg.data_type()),
                arg,
                Rc::new(|arg| {
                    MinAgg::new(arg).map(|agg| Box::new(agg) as Box<dyn Aggregator>)
                }),
            );
            Ok(Rc::new(function) as Rc<dyn Function>)
        } else {
            Err(DBError::Unknown("invalid args".to_string()))
        }
    }
}
