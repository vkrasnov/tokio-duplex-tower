use std::marker::PhantomData;

use serde::ser::SerializeTuple;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

use super::DuplexValue;

const RESPONSE_BIT: u8 = 1 << 7;

impl<Request, Response> Serialize for DuplexValue<Request, Response>
where
    Request: Serialize,
    Response: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut tup = serializer.serialize_tuple(2)?;
        match self {
            DuplexValue::Request(tag, request) => {
                tup.serialize_element(&tag)?;
                tup.serialize_element(request)?;
            }
            DuplexValue::Response(tag, response) => {
                // We differentiate between request and response based on the MSB
                tup.serialize_element(&(*tag | RESPONSE_BIT))?;
                tup.serialize_element(response)?;
            }
        };
        tup.end()
    }
}

impl<'de, Request, Response> Deserialize<'de> for DuplexValue<Request, Response>
where
    for<'d> Request: Deserialize<'d>,
    for<'d> Response: Deserialize<'d>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DuplexValueVisitor<Request, Response> {
            _req: PhantomData<Request>,
            _res: PhantomData<Response>,
        }

        impl<'de, Request, Response> de::Visitor<'de> for DuplexValueVisitor<Request, Response>
        where
            Request: Deserialize<'de>,
            Response: Deserialize<'de>,
        {
            type Value = DuplexValue<Request, Response>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a tuple of u8 and request/response")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
                A::Error: de::Error,
            {
                let tag: u8 = match seq.next_element()? {
                    Some(tag) => tag,
                    None => return Err(de::Error::invalid_length(0, &self)),
                };

                // The top bit of the tag tells us if that is a response or a request
                if tag & RESPONSE_BIT != 0 {
                    let response: Response = match seq.next_element()? {
                        Some(resp) => resp,
                        None => return Err(de::Error::invalid_length(1, &self)),
                    };
                    Ok(DuplexValue::Response(tag & !RESPONSE_BIT, response))
                } else {
                    let request: Request = match seq.next_element()? {
                        Some(req) => req,
                        None => return Err(de::Error::invalid_length(1, &self)),
                    };
                    Ok(DuplexValue::Request(tag, request))
                }
            }
        }

        deserializer.deserialize_tuple(
            2,
            DuplexValueVisitor {
                _req: Default::default(),
                _res: Default::default(),
            },
        )
    }
}
