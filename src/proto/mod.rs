//! data channel 上的二进制编码：protobuf wire 读取和外层信封。

mod envelope;
mod wire;

pub use envelope::envelope;
pub use wire::{read_fields, Field, Value};
