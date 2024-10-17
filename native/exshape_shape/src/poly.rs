use rustler::{Decoder, Encoder, Error, NifResult, Term};

use crate::ring::{ElixirRing, Ring};

pub struct Poly<'a> {
    rings: Vec<Ring<'a>>
}

pub struct ElixirPoly<'a> {
    rings: Vec<ElixirRing<'a>>
}

impl <'a> From<ElixirPoly<'a>> for Poly<'a> {
    fn from(value: ElixirPoly<'a>) -> Self {
        Self {
            rings: value.rings.into_iter().map(Ring::from).collect()
        }
    }
}

impl <'a> From<Poly<'a>> for ElixirPoly<'a> {
    fn from(value: Poly<'a>) -> Self {
        Self {
            rings: value.rings.into_iter().map(ElixirRing::from).collect()
        }
    }
}

impl <'a> Poly<'a> {
    pub fn first_ring(&self) -> &Ring<'a> {
        &self.rings[0]
    }

    pub fn push(&mut self, ring: Ring<'a>) {
        self.rings.push(ring)
    }
}

impl <'a> From<Ring<'a>> for Poly<'a> {
    fn from(ring: Ring<'a>) -> Self {
        Self { rings: vec![ring] }
    }
}

impl <'a> Decoder<'a> for ElixirPoly<'a> {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let rings = term.decode::<Vec<_>>()?;
        if rings.is_empty() {
            return Err(Error::BadArg);
        }
        Ok(ElixirPoly { rings })
    }
}

impl <'a> Encoder for ElixirPoly<'a> {
    fn encode<'b>(&self, env: rustler::Env<'b>) -> Term<'b> {
        self.rings.encode(env)
    }
}
