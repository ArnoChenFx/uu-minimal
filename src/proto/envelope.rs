//! data channel 上所有 protobuf 消息共用的外层信封：
//! `Message { 1 sequence, 2 timestamp, <which>: payload }`。

/// base-128 变长整数。
fn push_int(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let low = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(low);
            return;
        }
        out.push(low | 0x80);
    }
}

/// 字段键 = 字段号 << 3 | wire type。
fn push_tag(out: &mut Vec<u8>, number: u32, wire_type: u64) {
    push_int(out, (u64::from(number) << 3) | wire_type);
}

/// 长度前缀字段（wire type 2）。
fn push_blob(out: &mut Vec<u8>, number: u32, bytes: &[u8]) {
    push_tag(out, number, 2);
    push_int(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// `which` 是消息体所在的字段号（21 RpcRequest、22 RpcResponse、27 端口映射…）。
/// `sequence` / `timestamp` 为 0 时按 proto3 默认值省略。
pub fn envelope(sequence: u64, timestamp: u64, which: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 12);
    if sequence != 0 {
        push_tag(&mut out, 1, 0);
        push_int(&mut out, sequence);
    }
    if timestamp != 0 {
        push_tag(&mut out, 2, 0);
        push_int(&mut out, timestamp);
    }
    push_blob(&mut out, which, payload);
    out
}

#[cfg(test)]
mod tests {
    use super::envelope;
    use crate::proto::{read_fields, Value};

    #[test]
    fn omits_default_sequence_and_timestamp() {
        assert_eq!(envelope(0, 0, 27, b"x"), vec![0xda, 0x01, 1, b'x']);
    }

    #[test]
    fn field_number_carries_the_payload() {
        // 1 = seq, 2 = ts, 27 = 0xd0 >> ... 编码后是 0xda 0x01。
        assert_eq!(envelope(1, 2, 27, b"x"), vec![8, 1, 16, 2, 0xda, 0x01, 1, b'x']);
        let wrapped = envelope(3, 4, 22, b"body");
        let fields = read_fields(&wrapped).unwrap();
        assert_eq!(fields[0].int(), Some(3));
        assert_eq!(fields[1].int(), Some(4));
        assert_eq!(fields[2].number, 22);
        assert_eq!(fields[2].value, Value::Bytes(b"body"));
    }
}
