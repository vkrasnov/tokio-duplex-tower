use std::marker::PhantomData;

use bincode::Options;
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

const MAX_LEN_BYTES: usize = 10; // For variable encoded length, we store 7 bits per byte, for a max of u64

/// A length delimited frame codec, with an efficient varint length encoding and no length
/// restrictions (in practice at least). Serializes the inner data with bincode.
pub(crate) struct FrameCodec<I> {
    _item: PhantomData<I>,
}

impl<I> FrameCodec<I> {
    pub fn new() -> Self {
        FrameCodec {
            _item: Default::default(),
        }
    }
}

impl<I> Default for FrameCodec<I> {
    fn default() -> Self {
        Self::new()
    }
}

#[inline(always)]
/// Decode varint length, the scheme is to keep 7 bits of info per byte, and have the top bit
/// of each byte indicate if more bytes are needed. The returned value is a tuple of the total frame
/// length and the length of the varint encoded legth part.
fn decode_len(mut src: &[u8]) -> Option<(usize, usize)> {
    let mut len = 0usize;
    let mut lenlen = 1usize;
    while !src.is_empty() {
        len = (len << 7) | (src[0] & 0x7f) as usize;
        if src[0] >> 7 == 0 {
            return Some((len + lenlen, lenlen));
        }
        lenlen += 1;
        src = &src[1..];
    }

    None
}

/// Encode length as a varint. The scheme is to keep 7 bits of info per byte, and have the top bit
/// of each byte indicate if more bytes are needed.
/// For example 55 will be enocded as |0x37|, 200 will be encoded as |0x81|0x48|, where 0x48 are
/// basically bits [0..6], the 1 in 0x81 is bit 7 and the top bit set in 0x81 just indicates that
/// another bytes is required to fully decode (that byte in this case is 0x48, with the top bit
/// cleared, indicating this is the last byte).
#[inline(always)]
fn encode_len(mut len: u64, dst: &mut BytesMut) {
    // First encode into a temporary buffer
    let mut encoding = [0u8; MAX_LEN_BYTES];
    let mut idx = MAX_LEN_BYTES;

    loop {
        // Store 7 bits per byte
        let mut next_byte = (len & 0x7f) as u8;
        len >>= 7;

        if idx != MAX_LEN_BYTES {
            // If this is not the last byte, we set the top bit which indicates more bytes are
            // following
            next_byte |= 0x80;
        }

        idx -= 1;
        encoding[idx] = next_byte;

        if len == 0 {
            dst.put_slice(&encoding[idx..]);
            return;
        }
    }
}

impl<I> Decoder for FrameCodec<I>
where
    for<'de> I: Deserialize<'de>,
{
    type Item = I;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let (len, lenlen) = match decode_len(src) {
            None => return Ok(None),
            Some(len) => len,
        };

        if src.len() < len {
            // The full frame has not yet arrived.
            // We reserve more space in the buffer. This is not strictly
            // necessary, but is a good idea performance-wise.
            src.reserve(len - src.len());
            // We inform the Framed that we need more bytes to form the next
            // frame.
            return Ok(None);
        }

        let data = &src[lenlen..len];
        let serde_result = bincode::DefaultOptions::new().deserialize(data);
        src.advance(len);

        match serde_result {
            Ok(v) => Ok(Some(v)),
            Err(bincode_err) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                bincode_err,
            )),
        }
    }
}

impl<I> Encoder<I> for FrameCodec<I>
where
    I: Serialize,
{
    type Error = std::io::Error;

    fn encode(&mut self, item: I, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let enc_len = bincode::DefaultOptions::new()
            .serialized_size(&item)
            .expect("No length limit");

        dst.reserve(enc_len as usize + MAX_LEN_BYTES);

        encode_len(enc_len, dst);

        let mut writer = dst.writer();
        bincode::DefaultOptions::new()
            .serialize_into(&mut writer, &item)
            .expect("No length limit");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn single_frame(v: String) {
            let mut codec = FrameCodec::new();
            let mut buf = BytesMut::new();

            codec.encode(v.clone(), &mut buf).unwrap();
            // assert that decoder can decode whatever it encodes
            prop_assert_eq!(codec.decode(&mut buf.clone()).unwrap().unwrap(), v);

            proptest!(move |(b in 0usize..buf.len())| {
                // assert that when missing bytes, decoder asks for more bytes
                let mut codec = FrameCodec::<String>::new();
                let mut buf = buf.clone();
                buf.truncate(b);
                prop_assert!(codec.decode(&mut buf).unwrap().is_none());
            });
        }


        #[test]
        fn multi_frame(v: Vec<String>) {
            let mut codec = FrameCodec::<String>::new();
            let mut buf = BytesMut::new();

            for s in &v {
                codec.encode(s.clone(), &mut buf).unwrap();
            }

            let mut dec = Vec::new();

            while let Some(f) = codec.decode(&mut buf).unwrap() {
                dec.push(f);
            }

            prop_assert_eq!(v, dec);
        }
    }
}
