// Copyright 2025 FastLabs Developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;

use crate::ObjectStatus;

/// The result returned by `Pool::retain`.
#[derive(Debug)]
#[non_exhaustive]
pub struct RetainResult<T> {
    /// The number of retained objects.
    pub retained: usize,
    /// The objects removed from the pool.
    pub removed: Vec<T>,
}

pub(crate) trait SealedState {
    type Object;

    fn status(&self) -> ObjectStatus;
    fn mut_object(&mut self) -> &mut Self::Object;
    fn take_object(self) -> Self::Object;
}

pub(crate) fn do_vec_deque_retain<T, State: SealedState<Object = T>>(
    deque: &mut VecDeque<State>,
    mut f: impl FnMut(&mut T, ObjectStatus) -> bool,
) -> RetainResult<T> {
    let len = deque.len();
    let mut idx = 0;
    let mut cur = 0;

    // Stage 1: All values are retained.
    while cur < len {
        let state = &mut deque[cur];
        let status = state.status();
        if !f(state.mut_object(), status) {
            cur += 1;
            break;
        }
        cur += 1;
        idx += 1;
    }

    // Stage 2: Swap retained value into current idx.
    while cur < len {
        let state = &mut deque[cur];
        let status = state.status();
        if !f(state.mut_object(), status) {
            cur += 1;
            continue;
        }

        deque.swap(idx, cur);
        cur += 1;
        idx += 1;
    }

    // Stage 3: Truncate all values after idx.
    let removed = if cur != idx {
        let removed = deque.split_off(idx);
        removed.into_iter().map(State::take_object).collect()
    } else {
        Vec::new()
    };

    RetainResult {
        retained: idx,
        removed,
    }
}
