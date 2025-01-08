//! Transport from Cockroach Source to Arrow2 Destination.

use crate::destinations::arrow2::{
    typesystem::{
        Arrow2TypeSystem, DateTimeWrapperMicro, NaiveDateTimeWrapperMicro, NaiveTimeWrapperMicro,
    },
    Arrow2Destination, Arrow2DestinationError,
};
use crate::sources::cockroach::{
    BinaryProtocol, CSVProtocol, CursorProtocol, CockroachSource, CockroachSourceError,
    CockroachTypeSystem, SimpleProtocol,
};
use crate::typesystem::TypeConversion;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use num_traits::ToPrimitive;
use postgres::NoTls;
use postgres_openssl::MakeTlsConnector;
use rust_decimal::Decimal;
use serde_json::Value;
use std::marker::PhantomData;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum CockroachArrow2TransportError {
    #[error(transparent)]
    Source(#[from] CockroachSourceError),

    #[error(transparent)]
    Destination(#[from] Arrow2DestinationError),

    #[error(transparent)]
    ConnectorX(#[from] crate::errors::ConnectorXError),
}

/// Convert Cockroach data types to Arrow2 data types.
pub struct CockroachArrow2Transport<P, C>(PhantomData<P>, PhantomData<C>);

macro_rules! impl_cockroach_transport {
    ($proto:ty, $tls:ty) => {
        impl_transport!(
            name = CockroachArrow2Transport<$proto, $tls>,
            error = CockroachArrow2TransportError,
            systems = CockroachTypeSystem => Arrow2TypeSystem,
            route = CockroachSource<$proto, $tls> => Arrow2Destination,
            mappings = {
                { Float4[f32]                       => Float32[f32]                | conversion auto }
                { Float8[f64]                       => Float64[f64]                | conversion auto }
                { Numeric[Decimal]                  => Float64[f64]                | conversion option }
                { Int2[i16]                         => Int32[i32]                  | conversion auto }
                { Int4[i32]                         => Int32[i32]                  | conversion auto }
                { Int8[i64]                         => Int64[i64]                  | conversion auto }
                { Bool[bool]                        => Boolean[bool]               | conversion auto  }
                { Text[&'r str]                     => LargeUtf8[String]           | conversion owned }
                { BpChar[&'r str]                   => LargeUtf8[String]           | conversion none }
                { VarChar[&'r str]                  => LargeUtf8[String]           | conversion none }
                { Enum[&'r str]                     => LargeUtf8[String]           | conversion none }
                { Name[&'r str]                     => LargeUtf8[String]           | conversion none }
                { Timestamp[NaiveDateTime]          => Date64Micro[NaiveDateTimeWrapperMicro] | conversion option }
                { Date[NaiveDate]                   => Date32[NaiveDate]           | conversion auto }
                { Time[NaiveTime]                   => Time64Micro[NaiveTimeWrapperMicro]     | conversion option }
                { TimestampTz[DateTime<Utc>]        => DateTimeTzMicro[DateTimeWrapperMicro]  | conversion option }
                { UUID[Uuid]                        => LargeUtf8[String]           | conversion option }
                { Char[&'r str]                     => LargeUtf8[String]           | conversion none }
                { ByteA[Vec<u8>]                    => LargeBinary[Vec<u8>]        | conversion auto }
                { JSON[Value]                       => LargeUtf8[String]           | conversion option }
                { JSONB[Value]                      => LargeUtf8[String]           | conversion none }
                { BoolArray[Vec<bool>]              => BoolArray[Vec<bool>]        | conversion auto_vec }
                { Int2Array[Vec<i16>]               => Int64Array[Vec<i64>]        | conversion auto_vec }
                { Int4Array[Vec<i32>]               => Int64Array[Vec<i64>]        | conversion auto_vec }
                { Int8Array[Vec<i64>]               => Int64Array[Vec<i64>]        | conversion auto }
                { Float4Array[Vec<f32>]             => Float64Array[Vec<f64>]      | conversion auto_vec }
                { Float8Array[Vec<f64>]             => Float64Array[Vec<f64>]      | conversion auto }
                { NumericArray[Vec<Decimal>]        => Float64Array[Vec<f64>]      | conversion option }
                { VarcharArray[Vec<String>]        => Utf8Array[Vec<String>]      | conversion none }
                { TextArray[Vec<String>]        => Utf8Array[Vec<String>]      | conversion auto }

            }
        );
    }
}

impl_cockroach_transport!(BinaryProtocol, NoTls);
impl_cockroach_transport!(BinaryProtocol, MakeTlsConnector);
impl_cockroach_transport!(CSVProtocol, NoTls);
impl_cockroach_transport!(CSVProtocol, MakeTlsConnector);
impl_cockroach_transport!(CursorProtocol, NoTls);
impl_cockroach_transport!(CursorProtocol, MakeTlsConnector);
impl_cockroach_transport!(SimpleProtocol, NoTls);
impl_cockroach_transport!(SimpleProtocol, MakeTlsConnector);

impl<P, C> TypeConversion<NaiveTime, NaiveTimeWrapperMicro> for CockroachArrow2Transport<P, C> {
    fn convert(val: NaiveTime) -> NaiveTimeWrapperMicro {
        NaiveTimeWrapperMicro(val)
    }
}

impl<P, C> TypeConversion<NaiveDateTime, NaiveDateTimeWrapperMicro>
for CockroachArrow2Transport<P, C>
{
    fn convert(val: NaiveDateTime) -> NaiveDateTimeWrapperMicro {
        NaiveDateTimeWrapperMicro(val)
    }
}

impl<P, C> TypeConversion<DateTime<Utc>, DateTimeWrapperMicro> for CockroachArrow2Transport<P, C> {
    fn convert(val: DateTime<Utc>) -> DateTimeWrapperMicro {
        DateTimeWrapperMicro(val)
    }
}

impl<P, C> TypeConversion<Uuid, String> for CockroachArrow2Transport<P, C> {
    fn convert(val: Uuid) -> String {
        val.to_string()
    }
}

impl<P, C> TypeConversion<Decimal, f64> for CockroachArrow2Transport<P, C> {
    fn convert(val: Decimal) -> f64 {
        val.to_f64()
            .unwrap_or_else(|| panic!("cannot convert decimal {:?} to float64", val))
    }
}

impl<P, C> TypeConversion<Vec<Decimal>, Vec<f64>> for CockroachArrow2Transport<P, C> {
    fn convert(val: Vec<Decimal>) -> Vec<f64> {
        val.into_iter()
            .map(|v| {
                v.to_f64()
                    .unwrap_or_else(|| panic!("cannot convert decimal {:?} to float64", v))
            })
            .collect()
    }
}

impl<P, C> TypeConversion<Value, String> for CockroachArrow2Transport<P, C> {
    fn convert(val: Value) -> String {
        val.to_string()
    }
}
