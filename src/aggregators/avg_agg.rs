use crate::{
    data_types::DataType,
    expressions::{BinaryOp, Expression, Literal},
    interpreter::Interpreter,
    optimizer::{rules::ResolveExprRule, OptimizerContextForExpr},
    row::Row,
    tables::{FieldInfo, RelationSchema},
    DBError, DBResult,
};

use super::Aggregator;

const FIELD_AVG_SUM: &str = "avg_sum";
const FIELD_AVG_COUNT: &str = "avg_count";

pub struct AvgAgg {
    count_expr: Expression,
    sum_expr: Expression,
}

impl AvgAgg {
    pub fn new(input: &Expression) -> DBResult<Self> {
        let sum_expr = Expression::BinaryOp {
            op: BinaryOp::Plus,
            left: Box::new(input.clone()),
            right: Box::new(Expression::UnResolvedFieldRef(FIELD_AVG_SUM.to_owned())),
        };
        let count_expr = Expression::BinaryOp {
            op: BinaryOp::Plus,
            left: Box::new(Expression::Literal(Literal::UInt64(1))),
            right: Box::new(Expression::UnResolvedFieldRef(FIELD_AVG_COUNT.to_owned())),
        };
        let agg = Self {
            count_expr,
            sum_expr,
        };
        Ok(agg)
    }
}

impl Aggregator for AvgAgg {
    fn initial_row(&self) -> DBResult<Row<'static>> {
        Ok(Row::new(vec![Literal::Float64(0.0), Literal::UInt64(0)]))
    }

    fn resolve_expr(&mut self, inbound_schema: &RelationSchema) -> DBResult<()> {
        let extended_fields = [
            FieldInfo::new(FIELD_AVG_SUM.to_owned(), DataType::Float64),
            FieldInfo::new(FIELD_AVG_COUNT.to_owned(), DataType::UInt64),
        ];

        let new_fields = inbound_schema
            .get_fields()
            .iter()
            .chain(extended_fields.iter())
            .cloned()
            .collect();
        let schema = RelationSchema::new(new_fields);
        let context = OptimizerContextForExpr::new(schema);
        let new_sum_expr = self
            .sum_expr
            .transform_bottom_up(&context, ResolveExprRule::resolve_expression)?
            .ok_or(DBError::InterpretingError(
                "Cannot resolve avg agg expr".to_string(),
            ))?;
        let new_count_expr = self
            .count_expr
            .transform_bottom_up(&context, ResolveExprRule::resolve_expression)?
            .ok_or(DBError::InterpretingError(
                "Cannot resolve avg agg expr".to_string(),
            ))?;
        // TODO: implement cast rule
        self.sum_expr = new_sum_expr;
        self.count_expr = new_count_expr;
        Ok(())
    }

    fn process(&self, input_row: &Row, result_row: &mut Row) -> DBResult<()> {
        let target = Row::concat(input_row, result_row);
        let count = Interpreter::eval(&self.count_expr, &target)?;
        let sum = Interpreter::eval(&self.sum_expr, &target)?;

        result_row.update_field(0, sum)?;
        result_row.update_field(1, count)?;
        Ok(())
    }

    fn result(&self, result_row: &Row) -> DBResult<Literal> {
        match (result_row.get_field(0)?, result_row.get_field(1)?) {
            (Literal::Float64(sum), Literal::UInt64(count)) => {
                Ok(Literal::Float64(sum / (count as f64)))
            }
            _ => Err(DBError::Unknown("should never happen.".to_owned())),
        }
    }
}
