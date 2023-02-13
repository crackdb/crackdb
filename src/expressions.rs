use core::slice;
use std::borrow::Borrow;
use std::fmt::Display;
use std::hash::Hash;
use std::rc::Rc;

use crate::data_types::DataType;
use crate::functions::Function;
use crate::optimizer::{OptimizerContextForExpr, OptimizerNode};
use crate::DBError;
use crate::DBResult;

#[derive(Debug, Clone)]
pub enum Expression {
    Literal(Literal),
    UnResolvedFieldRef(String),
    FieldRef {
        name: String,
        index: usize,
        data_type: DataType,
    },
    Alias {
        alias: String,
        child: Box<Expression>,
    },
    BinaryOp {
        op: BinaryOp,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    UnaryOp {
        op: UnaryOp,
        input: Box<Expression>,
    },
    UnResolvedFunction {
        name: String,
        args: Vec<Expression>,
    },
    // TODO: revisit the usage of Rc here
    Function(Rc<dyn Function>),
    Wildcard,
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Literal(l) => l.fmt(f),
            Expression::UnResolvedFieldRef(name) => name.fmt(f),
            Expression::FieldRef { name, .. } => name.fmt(f),
            Expression::Alias { alias, child: _ } => alias.fmt(f),
            Expression::BinaryOp { op, left, right } => {
                write!(f, "{left}_{op}_{right}")
            }
            Expression::UnaryOp { op, input } => write!(f, "{op}_{input}"),
            Expression::UnResolvedFunction { name, args } => write!(
                f,
                "{name}({})",
                args.iter()
                    .map(|e| { e.to_string() })
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            Expression::Function(func) => func.as_ref().to_expr_string().fmt(f),
            Expression::Wildcard => "*".fmt(f),
        }
    }
}

#[derive(Debug, Clone)]
pub enum BinaryOp {
    Plus,
    Minus,
    Divide,
    Multiply,
    Gt,
    Gte,
    Eq,
    Lt,
    Lte,
    And,
    Or,
    Max,
    Min,
}

impl Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOp::Plus => "+".fmt(f),
            BinaryOp::Minus => "-".fmt(f),
            BinaryOp::Divide => "/".fmt(f),
            BinaryOp::Multiply => "*".fmt(f),
            BinaryOp::Gt => ">".fmt(f),
            BinaryOp::Gte => ">=".fmt(f),
            BinaryOp::Eq => "=".fmt(f),
            BinaryOp::Lt => "<".fmt(f),
            BinaryOp::Lte => "<=".fmt(f),
            BinaryOp::And => "AND".fmt(f),
            BinaryOp::Or => "OR".fmt(f),
            BinaryOp::Max => "MAX".fmt(f),
            BinaryOp::Min => "MIN".fmt(f),
        }
    }
}

impl BinaryOp {
    pub fn is_boolean_op(&self) -> bool {
        matches!(
            self,
            BinaryOp::Gt
                | BinaryOp::Gte
                | BinaryOp::Eq
                | BinaryOp::Lt
                | BinaryOp::Lte
                | BinaryOp::And
                | BinaryOp::Or
        )
    }
}

#[derive(Debug, Clone)]
pub enum UnaryOp {
    Not,
    Neg,
}

impl Display for UnaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryOp::Not => "NOT".fmt(f),
            UnaryOp::Neg => "-".fmt(f),
        }
    }
}

impl OptimizerNode for Expression {
    type Context = OptimizerContextForExpr;
}

impl Expression {
    pub fn data_type(&self) -> DataType {
        match self {
            Expression::Literal(l) => l.data_type(),
            Expression::UnResolvedFieldRef(_) => DataType::Unknown,
            Expression::FieldRef { data_type, .. } => data_type.clone(),
            Expression::BinaryOp { op, left, right: _ } => match op {
                op if op.is_boolean_op() => DataType::Boolean,
                _ => left.data_type(),
            },
            Expression::UnaryOp { input, .. } => input.data_type(),
            Expression::Alias { alias: _, child } => child.data_type(),
            // TODO: implement this
            Expression::UnResolvedFunction { name: _, args: _ } => DataType::Unknown,
            Expression::Function(func) => func.as_ref().data_type(),
            Expression::Wildcard => DataType::Unknown,
        }
    }

    /// id for the expression that can be used to check the semantic equality of two exprs,
    /// e.g.: `SUM(X) as sum_x` is semantically equal to `SUM(X)`
    pub fn sematic_id(&self) -> String {
        match self {
            Expression::Literal(l) => format!("literal_{}#{}", l, l.data_type()),
            Expression::UnResolvedFieldRef(f) => format!("unresolved_ref_#{f}"),
            Expression::FieldRef {
                name,
                index: _,
                data_type,
            } => format!("ref_{name}#{data_type}"),
            Expression::Alias { alias: _, child } => child.sematic_id(),
            Expression::BinaryOp { op, left, right } => {
                format!("{}_{}_{}", left.sematic_id(), op, right.sematic_id())
            }
            Expression::UnaryOp { op, input } => format!("{op}_{}", input.sematic_id()),
            Expression::UnResolvedFunction { name: _, args: _ } => self.to_string(),
            Expression::Function(_) => self.to_string(),
            Expression::Wildcard => self.to_string(),
        }
    }

    pub fn transform_bottom_up<T>(
        &self,
        context: &OptimizerContextForExpr,
        func: &mut T,
    ) -> DBResult<Option<Expression>>
    where
        T: FnMut(&Expression, &OptimizerContextForExpr) -> DBResult<Option<Expression>>,
    {
        match self {
            Expression::Literal(_) => func(self, context),
            Expression::UnResolvedFieldRef(_) => func(self, context),
            Expression::FieldRef { .. } => func(self, context),
            Expression::BinaryOp { op, left, right } => {
                let children = vec![left.as_ref(), right.as_ref()];
                self.transform_bottom_up_helper(&children, context, func, |children| {
                    let mut iter = children.into_iter();
                    Ok(Expression::BinaryOp {
                        op: op.clone(),
                        left: Box::new(iter.next().unwrap()),
                        right: Box::new(iter.next().unwrap()),
                    })
                })
            }
            Expression::UnaryOp { op, input } => {
                let children = slice::from_ref(input);
                self.transform_bottom_up_helper(&children, context, func, |children| {
                    let mut iter = children.into_iter();
                    Ok(Expression::UnaryOp {
                        op: op.clone(),
                        input: Box::new(iter.next().unwrap()),
                    })
                })
            }
            Expression::Alias { alias, child } => {
                let children = slice::from_ref(child.as_ref());
                self.transform_bottom_up_helper(&children, context, func, |children| {
                    let mut iter = children.into_iter();
                    Ok(Expression::Alias {
                        alias: alias.clone(),
                        child: Box::new(iter.next().unwrap()),
                    })
                })
            }
            Expression::UnResolvedFunction { name, args } => self
                .transform_bottom_up_helper(args, context, func, |children| {
                    Ok(Expression::UnResolvedFunction {
                        name: name.clone(),
                        args: children,
                    })
                }),
            Expression::Function(f) => {
                let args = f.args();
                self.transform_bottom_up_helper(&args, context, func, |args| {
                    f.with_args(args).map(Expression::Function)
                })
            }
            Expression::Wildcard => func(self, context),
        }
    }

    fn transform_bottom_up_helper<T, B>(
        &self,
        children: &[impl Borrow<Expression>],
        context: &OptimizerContextForExpr,
        func: &mut T,
        builder: B,
    ) -> DBResult<Option<Expression>>
    where
        T: FnMut(&Expression, &OptimizerContextForExpr) -> DBResult<Option<Expression>>,
        B: Fn(Vec<Expression>) -> DBResult<Expression>,
    {
        let mut any_children_updated = false;
        let mut updated_children = Vec::new();
        for child in children {
            if let Some(updated) = child.borrow().transform_bottom_up(context, func)? {
                any_children_updated = true;
                updated_children.push(updated);
            } else {
                updated_children.push(child.borrow().clone());
            }
        }

        if any_children_updated {
            let updated_self = builder(updated_children)?;
            match func(&updated_self, context)? {
                Some(updated_self) => Ok(Some(updated_self)),
                None => Ok(Some(updated_self)),
            }
        } else {
            func(self, context)
        }
    }

    fn children(&self) -> Vec<&Expression> {
        match self {
            Expression::Literal(_) => vec![],
            Expression::UnResolvedFieldRef(_) => vec![],
            Expression::FieldRef {
                name: _,
                index: _,
                data_type: _,
            } => vec![],
            Expression::Alias { alias: _, child } => vec![child],
            Expression::BinaryOp { op: _, left, right } => vec![left, right],
            Expression::UnaryOp { op: _, input } => vec![input],
            Expression::UnResolvedFunction { name: _, args } => args.iter().collect(),
            Expression::Function(f) => f.args(),
            Expression::Wildcard => vec![],
        }
    }

    fn clone_with_children(&self, children: Vec<Expression>) -> DBResult<Expression> {
        match self {
            Expression::Literal(_) => Err(DBError::should_never_happen()),
            Expression::UnResolvedFieldRef(_) => Err(DBError::should_never_happen()),
            Expression::FieldRef {
                name: _,
                index: _,
                data_type: _,
            } => Err(DBError::should_never_happen()),
            Expression::Alias { alias, child: _ } => Ok(Expression::Alias {
                alias: alias.clone(),
                child: Box::new(children.into_iter().next().unwrap()),
            }),
            Expression::BinaryOp {
                op,
                left: _,
                right: _,
            } => {
                let mut iter = children.into_iter();
                Ok(Expression::BinaryOp {
                    op: op.clone(),
                    left: Box::new(iter.next().unwrap()),
                    right: Box::new(iter.next().unwrap()),
                })
            }
            Expression::UnaryOp { op, input: _ } => Ok(Expression::UnaryOp {
                op: op.clone(),
                input: Box::new(children.into_iter().next().unwrap()),
            }),
            Expression::UnResolvedFunction { name, args: _ } => {
                Ok(Expression::UnResolvedFunction {
                    name: name.clone(),
                    args: children,
                })
            }
            Expression::Function(f) => {
                let updated_f = f.with_args(children)?;
                Ok(Expression::Function(updated_f))
            }
            Expression::Wildcard => Ok(Expression::Wildcard),
        }
    }

    pub fn transform_top_down<T>(
        &self,
        context: &OptimizerContextForExpr,
        func: &mut T,
    ) -> DBResult<Option<Expression>>
    where
        T: FnMut(&Expression, &OptimizerContextForExpr) -> DBResult<Option<Expression>>,
    {
        match func(self, context)? {
            Some(updated) => {
                let maybe_updated_children = updated
                    .children()
                    .iter()
                    .map(|child| child.transform_top_down(context, func))
                    .collect::<DBResult<Option<Vec<_>>>>()?;
                match maybe_updated_children {
                    Some(updated_children) if !updated_children.is_empty() => {
                        updated.clone_with_children(updated_children).map(Some)
                    }
                    _ => Ok(Some(updated)),
                }
            }
            None => {
                let children = self.children();
                if !children.is_empty() {
                    let mut child_updated = false;
                    let mut maybe_updated_children = Vec::with_capacity(children.len());
                    for child in children {
                        if let Some(updated) = child.transform_top_down(context, func)? {
                            child_updated = true;
                            maybe_updated_children.push(updated);
                        } else {
                            maybe_updated_children.push(child.clone());
                        }
                    }
                    if child_updated {
                        return Ok(Some(
                            self.clone_with_children(maybe_updated_children)?,
                        ));
                    }
                }
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Literal {
    UnResolvedNumber(String),
    UnResolvedString(String),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    String(String),
    DateTime(String),
    Null,
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::UnResolvedNumber(l0), Self::UnResolvedNumber(r0)) => l0 == r0,
            (Self::UnResolvedString(l0), Self::UnResolvedString(r0)) => l0 == r0,
            (Self::UInt8(l0), Self::UInt8(r0)) => l0 == r0,
            (Self::UInt16(l0), Self::UInt16(r0)) => l0 == r0,
            (Self::UInt32(l0), Self::UInt32(r0)) => l0 == r0,
            (Self::UInt64(l0), Self::UInt64(r0)) => l0 == r0,
            (Self::Int8(l0), Self::Int8(r0)) => l0 == r0,
            (Self::Int16(l0), Self::Int16(r0)) => l0 == r0,
            (Self::Int32(l0), Self::Int32(r0)) => l0 == r0,
            (Self::Int64(l0), Self::Int64(r0)) => l0 == r0,
            (Self::Float32(l0), Self::Float32(r0)) => l0 == r0,
            (Self::Float64(l0), Self::Float64(r0)) => l0 == r0,
            (Self::Bool(l0), Self::Bool(r0)) => l0 == r0,
            (Self::String(l0), Self::String(r0)) => l0 == r0,
            (Self::DateTime(l0), Self::DateTime(r0)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

/// this is hack for using Literal as HashMap key
/// FIXME: deal with NaN
impl Eq for Literal {
    fn assert_receiver_is_total_eq(&self) {}
}

/// TODO: revisit this to make sure floats are safe for hashing
impl Hash for Literal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::UnResolvedNumber(n) => n.fmt(f),
            Literal::UnResolvedString(s) => s.fmt(f),
            Literal::UInt8(v) => v.fmt(f),
            Literal::UInt16(v) => v.fmt(f),
            Literal::UInt32(v) => v.fmt(f),
            Literal::UInt64(v) => v.fmt(f),
            Literal::Int8(v) => v.fmt(f),
            Literal::Int16(v) => v.fmt(f),
            Literal::Int32(v) => v.fmt(f),
            Literal::Int64(v) => v.fmt(f),
            Literal::Float32(v) => v.fmt(f),
            Literal::Float64(v) => v.fmt(f),
            Literal::Bool(v) => v.fmt(f),
            Literal::String(v) => v.fmt(f),
            Literal::DateTime(v) => v.fmt(f),
            Literal::Null => "null".fmt(f),
        }
    }
}

impl Literal {
    pub fn as_bool(&self) -> DBResult<bool> {
        match self {
            Literal::Bool(v) => Ok(*v),
            _ => Err(DBError::Unknown("Cannot convert int to bool.".to_string())),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Literal::UnResolvedNumber(_) => DataType::Unknown,
            Literal::UnResolvedString(_) => DataType::Unknown,
            Literal::UInt8(_) => DataType::UInt8,
            Literal::UInt16(_) => DataType::UInt16,
            Literal::UInt32(_) => DataType::UInt32,
            Literal::UInt64(_) => DataType::UInt64,
            Literal::Int8(_) => DataType::Int8,
            Literal::Int16(_) => DataType::Int16,
            Literal::Int32(_) => DataType::Int32,
            Literal::Int64(_) => DataType::Int64,
            Literal::Float32(_) => DataType::Float32,
            Literal::Float64(_) => DataType::Float64,
            Literal::Bool(_) => DataType::Boolean,
            Literal::String(_) => DataType::String,
            Literal::DateTime(_) => DataType::DateTime,
            Literal::Null => DataType::Unknown,
        }
    }

    /// cast literal to the given data type, or cast to a higher precesision data type in order to maintain precession if:
    /// 1. the literal already has higher precision than given data type
    /// 2. the literal will have higher precission after casting
    pub fn cast_or_maintain_precision(
        &self,
        data_type: DataType,
    ) -> DBResult<Option<Literal>> {
        match self {
            // for numbers:
            // 1. always cast to Float64 if number looks like float and data_type hint is int
            // 2. always cast to Int64 or UInt64 if the given data_type cannot satisfy the required precession when parsing
            Literal::UnResolvedNumber(v) => {
                if data_type.is_integer() && looks_like_float(v) {
                    let f = v.parse::<f64>()?;
                    Ok(Some(Literal::Float64(f)))
                } else {
                    parse_number(data_type, v)
                }
            }
            Literal::UnResolvedString(v) => match data_type {
                DataType::String => Ok(Some(Literal::String(v.to_string()))),
                DataType::DateTime => Ok(Some(Literal::DateTime(v.to_string()))),
                _ => Ok(None),
            },
            // TODO: add support for more castings
            _ => Ok(None),
        }
    }
}

/// parse string into given numeric data type
fn parse_number(data_type: DataType, v: &str) -> Result<Option<Literal>, DBError> {
    match data_type {
        DataType::UInt8 => {
            let u = v.parse::<u64>()?;
            if u >= u8::MIN as u64 && u <= u8::MAX as u64 {
                Ok(Some(Literal::UInt8(u as u8)))
            } else {
                Ok(Some(Literal::UInt64(u)))
            }
        }
        DataType::UInt16 => {
            let u = v.parse::<u64>()?;
            if u >= u16::MIN as u64 && u <= u16::MAX as u64 {
                Ok(Some(Literal::UInt16(u as u16)))
            } else {
                Ok(Some(Literal::UInt64(u)))
            }
        }
        DataType::UInt32 => {
            let u = v.parse::<u64>()?;
            if u >= u32::MIN as u64 && u <= u32::MAX as u64 {
                Ok(Some(Literal::UInt32(u as u32)))
            } else {
                Ok(Some(Literal::UInt64(u)))
            }
        }
        DataType::UInt64 => Ok(Some(Literal::UInt64(v.parse::<u64>()?))),
        DataType::Int8 => {
            let u = v.parse::<i64>()?;
            if u >= i8::MIN as i64 && u <= i8::MAX as i64 {
                Ok(Some(Literal::Int8(u as i8)))
            } else {
                Ok(Some(Literal::Int64(u)))
            }
        }
        DataType::Int16 => {
            let u = v.parse::<i64>()?;
            if u >= i16::MIN as i64 && u <= i16::MAX as i64 {
                Ok(Some(Literal::Int16(u as i16)))
            } else {
                Ok(Some(Literal::Int64(u)))
            }
        }
        DataType::Int32 => {
            let u = v.parse::<i64>()?;
            if u >= i32::MIN as i64 && u <= i32::MAX as i64 {
                Ok(Some(Literal::Int32(u as i32)))
            } else {
                Ok(Some(Literal::Int64(u)))
            }
        }
        DataType::Int64 => Ok(Some(Literal::Int64(v.parse::<i64>()?))),
        DataType::Float32 => {
            let f = v.parse::<f32>()?;
            Ok(Some(Literal::Float32(f)))
        }
        DataType::Float64 => {
            let f = v.parse::<f64>()?;
            Ok(Some(Literal::Float64(f)))
        }
        _ => Ok(None),
    }
}

/// determin if a str looks like float
fn looks_like_float(v: &str) -> bool {
    v.contains('.')
        && v.trim_end_matches('0')
            .split_once('.')
            .filter(|(_, decimal)| !decimal.is_empty())
            .is_some()
}
