# Fastpool

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![MSRV 1.85][msrv-badge]](https://www.whatrustisit.com)
[![Apache 2.0 licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/fastpool.svg
[crates-url]: https://crates.io/crates/fastpool
[docs-badge]: https://docs.rs/fastpool/badge.svg
[msrv-badge]: https://img.shields.io/badge/MSRV-1.85-green?logo=rust
[docs-url]: https://docs.rs/fastpool
[license-badge]: https://img.shields.io/crates/l/fastpool
[license-url]: LICENSE
[actions-badge]: https://github.com/fast/fastpool/workflows/CI/badge.svg
[actions-url]:https://github.com/fast/fastpool/actions?query=workflow%3ACI

## Overview

Fastpool provides fast and runtime-agnostic object pools for Async Rust.

You can read [the docs page](https://docs.rs/fastpool/*/fastpool/) for a complete overview of the library.

## Installation

Add the dependency to your `Cargo.toml` via:

```shell
cargo add fastpool
```

## Documentation

Read the online documents at https://docs.rs/fastpool.

## Minimum Supported Rust Version (MSRV)

This crate is built against the latest stable release, and its minimum supported rustc version is 1.85.0.

The policy is that the minimum Rust version required to use this crate can be increased in minor version updates. For example, if Fastpool 1.0 requires Rust 1.20.0, then Fastpool 1.0.z for all values of z will also require Rust 1.20.0 or newer. However, Fastpool 1.y for y > 0 may require a newer minimum version of Rust.

## License

This project is licensed under [Apache License, Version 2.0](LICENSE).

## Origins

This library is derived from the [deadpool](https://docs.rs/deadpool/) crate with several dedicated considerations and a quite different mindset.

You can read the FAQ section on [the docs page](https://docs.rs/fastpool/*/fastpool/#faq) for a detailed discussion on "Why does fastpool have no timeout config?" and "Why does fastpool have no before/after hooks?"

The [postgres example](examples/postgres) and [this issue thread](https://github.com/launchbadge/sqlx/issues/2276#issuecomment-2687157357) is a significant motivation for this crate:

* Keeps the crate runtime-agnostic (see also ["Why does fastpool have no timeout config?"](https://docs.rs/fastpool/*/fastpool/#why-does-fastpool-have-no-timeout-config))
* Keeps the abstraction really dead simple (see also ["Why does fastpool have no before/after hooks?"](https://docs.rs/fastpool/*/fastpool/#why-does-fastpool-have-no-beforeafter-hooks))
* Returns an `Arc<Pool>` on creation so that maintenance could be triggered with a weak reference. This helps applications to teardown (drop) the pool easily with Rust's built-in RAII mechanism. See also [this example](https://docs.rs/fastpool/*/fastpool/bounded/struct.Pool.html#method.retain).
