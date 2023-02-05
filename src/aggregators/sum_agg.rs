use crate::{
    data_types::DataType,
    expressions::{BinaryOp, Expression},
    interpreter::Interpreter,
    optimizer::{rules::ResolveExprRule, OptimizerContextForExpr},
    row::Row,
    tables::{FieldInfo, RelationSchema},
    DBError, DBResult,
};

use super::Aggregator;

const FIELD_SUM: &str = "sum_agg_sum";

pub struct SumAgg {
    sum_expr: Expression,
    data_type: DataType,
}

impl SumAgg {
    pub fn new(expr: &Expression) -> DBResult<Self> {
        let sum_expr = Expression::BinaryOp {
            op: BinaryOp::Plus,
            left: Box::new(expr.clone()),
            right: Box::new(Expression::UnResolvedFieldRef(FIELD_SUM.to_owned())),
        };
        let data_type = expr.data_type();
        Ok(Self {
            sum_expr,
            data_type,
        })
    }
}

impl Aggregator for SumAgg {
    fn initial_row(&self) -> DBResult<Row<'static>> {
        let zero = self.data_type.zero()?;
        Ok(Row::new(vec![zero]))
    }

    fn resolve_expr(
        &mut self,
        inbound_schema: &crate::tables::RelationSchema,
    ) -> crate::DBResult<()> {
        let extended_fields = [FieldInfo::new(FIELD_SUM.to_owned(), DataType::Float64)];
        let new_fields = inbound_schema
            .get_fields()
            .iter()
            .chain(extended_fields.iter())
            .cloned()
            .collect();
        let schema = RelationSchema::new(new_fields);
        let context = OptimizerContextForExpr::new(schema);
        let error = DBError::InterpretingError("Cannot resolve sum agg expr".to_string());
        let new_sum_expr = self
            .sum_expr
            .transform_bottom_up(&context, &mut ResolveExprRule::resolve_expression)?
            .ok_or(error)?;
        // TODO: implement cast rule
        self.sum_expr = new_sum_expr;
        Ok(())
    }

    fn process(&self, input_row: &Row, result_row: &mut Row) -> crate::DBResult<()> {
        let target = Row::concat(input_row, result_row);
        let literal = Interpreter::eval(&self.sum_expr, &target)?;
        result_row.update_field(0, literal)?;
        Ok(())
    }

    fn result(&self, result_row: &Row) -> crate::DBResult<crate::expressions::Literal> {
        result_row.get_field(0)
    }
}
