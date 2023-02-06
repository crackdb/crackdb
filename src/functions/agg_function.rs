use std::rc::Rc;

use crate::{
    aggregators::Aggregator, data_types::DataType, expressions::Expression, DBError,
    DBResult,
};

use super::Function;

pub type AggregatorBuilder = dyn Fn(&Expression) -> DBResult<Box<dyn Aggregator>>;
pub type DataTypeExtractor = dyn Fn(&Expression) -> DataType;

#[derive(Clone)]
pub struct AggFunction {
    name: String,
    data_type_extractor: Rc<DataTypeExtractor>,
    arg: Expression,
    agg_builder: Rc<AggregatorBuilder>,
}

impl std::fmt::Debug for AggFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggFunction")
            .field("name", &self.name)
            .field("data_type", &self.data_type())
            .field("arg", &self.arg)
            .finish()
    }
}

impl AggFunction {
    pub fn new(
        name: &str,
        data_type_extractor: Rc<DataTypeExtractor>,
        arg: Expression,
        agg_builder: Rc<AggregatorBuilder>,
    ) -> Self {
        Self {
            name: name.to_owned(),
            data_type_extractor,
            arg,
            agg_builder,
        }
    }
}

impl Function for AggFunction {
    fn is_aggregator(&self) -> bool {
        true
    }

    fn aggregator(&self) -> DBResult<Box<dyn Aggregator>> {
        (self.agg_builder)(&self.arg)
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn args(&self) -> Vec<&Expression> {
        vec![&self.arg]
    }

    fn data_type(&self) -> DataType {
        (self.data_type_extractor)(&self.arg)
    }

    fn with_args(&self, args: Vec<Expression>) -> DBResult<Rc<dyn Function>> {
        if args.len() == 1 {
            let arg = args.into_iter().next().unwrap();
            Ok(Rc::new(AggFunction {
                name: self.name.clone(),
                data_type_extractor: self.data_type_extractor.clone(),
                arg,
                agg_builder: self.agg_builder.clone(),
            }))
        } else {
            Err(DBError::Unknown("invalid args".to_string()))
        }
    }
}
