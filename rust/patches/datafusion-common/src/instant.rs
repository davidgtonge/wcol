// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! WASM-compatible `Instant` wrapper.

/// `wasm32-unknown-unknown` has no clock, and `web_time` would import JS.
/// Planning only uses this to time optimizer passes, so a stopped clock is enough.
#[cfg(target_family = "wasm")]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Instant(u64);

#[cfg(target_family = "wasm")]
impl Instant {
    pub fn now() -> Self {
        Instant(0)
    }

    pub fn elapsed(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }
}

#[cfg(target_family = "wasm")]
impl std::ops::Add<std::time::Duration> for Instant {
    type Output = Self;

    fn add(self, _rhs: std::time::Duration) -> Self {
        self
    }
}

#[cfg(target_family = "wasm")]
impl std::ops::Sub for Instant {
    type Output = std::time::Duration;

    fn sub(self, _rhs: Self) -> Self::Output {
        std::time::Duration::ZERO
    }
}

#[expect(clippy::disallowed_types)]
#[cfg(not(target_family = "wasm"))]
/// DataFusion wrapper around [`std::time::Instant`]. This is only a type alias.
pub type Instant = std::time::Instant;
