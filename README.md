# The Nimble File Format

[![Linux Build](https://github.com/facebookincubator/nimble/actions/workflows/linux-build.yml/badge.svg)](https://github.com/facebookincubator/nimble/actions/workflows/linux-build.yml)
[![Velox](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/srsuryadev/e3263c7396c5b33486ac167d8b52ec02/raw/67292508e2e4891ef4ff168bc69ac4176b9a4a7a/velox-status.json)](https://github.com/facebookincubator/velox)

Nimble (formerly known as _”Alpha”_) is a new columnar file format for large
datasets created by Meta. Nimble is meant to be a replacement for file formats
such as Apache Parquet and ORC.

Watch [this talk](https://www.youtube.com/watch?v=bISBNVtXZ6M) to learn more
about Nimble's internals.

Nimble has the following design principles:

- **Wide:** Nimble is better suited for workloads that are wide in nature, such
  as tables with thousands of columns (or streams) which are commonly found in
  feature engineering workloads and training tables for machine learning.

- **Extensible:** Since the state-of-the-art in data encoding evolves faster
  than the file layout itself, Nimble decouples stream encoding from the
  underlying physical layout. Nimble allows encodings to be extended by library
  users and recursively applied (cascading).

- **Parallel:** Nimble is meant to fully leverage highly parallel hardware by
  providing encodings which are SIMD and GPU friendly. Although this is not
  implemented yet, we intend to expose metadata to allow developers to better
  plan decoding trees and schedule kernels without requiring the data streams
  themselves.

- **Unified:** More than a specification, Nimble is a product. We strongly
  discourage developers to (re-)implement Nimble's spec to prevent environmental
  fragmentation issues observed with similar projects in the past. We encourage
  developers to leverage the single unified Nimble library, and create
  high-quality bindings to other languages as needed.

Nimble has the following features:

- Lighter metadata organization to efficiently support thousands to tens of
  thousands of columns and streams.

- Use Flatbuffers instead of thrift/protobuf to more efficiently access large
  metadata sections.

- Use block encoding instead of stream encoding to provide predictable memory
  usage while decoding/reading.

- Supports many encodings out-of-the-box, and additional encodings can be added
  as needed.

- Supports cascading (recursive/composite) encoding of streams.

- Supports pluggable encoding selection policies.

- Provide extensibility APIs where encodings and other aspects of the file can
  be extended.

- Clear separation between logical and physical encoded types.

- And more.

Nimble is a work in progress, and many of these features above are still under
design and/or active development. As such, Nimble does not provide stability or
versioning guarantees (yet). They will be eventually provided with a future
stable release. Use it at your own risk.

## Build

Nimble's CMake build system is self-sufficient and able to either locate its
main dependencies or compile them locally. In order to compile it, one can
simply:

```shell
$ git clone git@github.com:facebookincubator/nimble.git
$ cd nimble
$ make
```

To override the default behavior and force the build system to, for example,
build a dependency locally (bundle it), one can:

```shell
$ folly_SOURCE=BUNDLED make
```

Nimble builds have been tested using clang 15 and 16. It should automatically
compile the following dependencies: gtest, glog, folly, abseil, and velox. You
may need to first install the following system dependencies for these to compile
(example from Ubuntu 22.04):

```shell
$ sudo apt install -y \
    git \
    cmake \
    flatbuffers-compiler \
    protobuf-compiler \
    libflatbuffers-dev \
    libgflags-dev \
    libunwind-dev \
    libgoogle-glog-dev \
    libdouble-conversion-dev \
    libevent-dev \
    liblz4-dev \
    liblzo2-dev \
    libelf-dev \
    libdwarf-dev \
    libsnappy-dev \
    libssl-dev \
    bison \
    flex \
    libfl-dev \
    pkg-config \
    clang \
    clang-format
```

Although Nimble's codebase is today closely coupled with velox, we intend to
decouple them in the future.

## Advance Velox Version

Nimble integrates Velox as a Git submodule, referencing a specific commit of the
Velox repository. The Velox badge at the top of this README shows the current
commit and how far behind it is from Velox main.

[See what changed since the current Velox commit.](https://github.com/facebookincubator/velox/compare/6566eba401dc2e235b42e947867fba0004dd371a...main)
<!-- pre-commit check-velox-readme validates the SHA above matches the submodule -->

Advance Velox when your changes depend on code in Velox that
is not available in the current commit, or when the submodule falls too far
behind. To update the Velox version, follow these steps:

```bash
git -C velox checkout main
git -C velox pull
git add velox
```

Build and run tests to ensure everything works. The pre-commit hook will
automatically update the Velox compare link in this README. Submit a PR, get
it approved and merged.

## License

Nimble is licensed under the Apache 2.0 License. A copy of the license
[can be found here.](LICENSE)
