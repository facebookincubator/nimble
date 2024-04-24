# The Nimble File Format

Nimble (formerly known as *“Alpha”*) is a new columnar file format for large
datasets created by Meta. Nimble is meant to be a replacement for file formats
such as Apache Parquet and ORC. 

Watch [this talk](https://www.youtube.com/watch?v=bISBNVtXZ6M) to learn more
about Nimble’s internals.

Nimble has the following design principles:

* **Wide:** Nimble is better suited for workloads that are wide in nature, such
  as tables with thousands of columns (or streams) which are commonly found in
  feature engineering workloads and training tables for machine learning. 

* **Extensible:** Since the state-of-the-art in data encoding evolves faster
  than the file layout itself, Nimble decouples stream encoding from the
  underlying physical layout. Nimble allows encodings to be extended by library
  users and recursively applied (cascading). 

* **Parallel:** Nimble is meant to fully leverage highly parallel hardware by
  providing encodings which are SIMD and GPU friendly. Although this is not
  implemented yet, we intend to expose metadata to allow developers to better
  plan decoding trees and schedule kernels without requiring the data streams
  themselves. 

* **Unified:** More than a specification, Nimble is a product. We strongly
  discourage developers to (re-)implement Nimble’s spec to prevent
  environmental fragmentation issues observed with similar projects in the
  past. We encourage developers to leverage the single unified Nimble library,
  and create high-quality bindings to other languages as needed.

Nimble has the following features:

* Lighter metadata organization to efficiently support thousands to tens of
thousands of columns and streams.

* Use Flatbuffers instead of thrift/protobuf to more efficiently access large
  metadata sections. 

* Use block encoding instead of stream encoding to provide predictable memory
  usage while decoding/reading.

* Supports many encodings out-of-the-box, and additional encodings can be added
  as needed. 

* Supports cascading (recursive/composite) encoding of streams. 

* Supports pluggable encoding selection policies.

* Provide extensibility APIs where encodings and other aspects of the file can
  be extended. 

* Clear separation between logical and physical encoded types.

* And more.


Nimble is a work in progress, and many of these features above are still under
design and/or active development. As such, Nimble does not provide stability or
versioning guarantees (yet). They will be eventually provided with a future
stable release. Use it at your own risk. 

## Build

Nimble’s CMake build system is self-sufficient and able to either locate its
main dependencies or compile them locally. In order to compile it, one can simply:

```shell
$ git clone git@github.com:facebookexternal/nimble.git
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
may need to first install the following system dependencies for these to
compile (example from Ubuntu 22.04):

```shell
$ sudo apt install -y \
    flatbuffers-compiler \
    libflatbuffers-dev \
    libgflags-dev \
    libunwind-dev \
    libgoogle-glog-dev \
    libdouble-conversion-dev \
    libevent-dev \
    liblzo2-dev \
    libelf-dev \
    libdwarf-dev \
    libsnappy-dev \
    bison \
    flex \
    libfl-dev
```

Although Nimble’s codebase is today closely coupled with velox, we intend to decouple
them in the future.

## License

Nimble is licensed under the Apache 2.0 License. A copy of the license
[can be found here.](LICENSE)
