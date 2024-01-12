.PHONY: all cmake build clean debug release unit

BUILD_BASE_DIR=_build
BUILD_DIR=release
BUILD_TYPE=Release

CMAKE_FLAGS := -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)

# Use Ninja if available. If Ninja is used, pass through parallelism control flags.
USE_NINJA ?= 1
ifeq ($(USE_NINJA), 1)
ifneq ($(shell which ninja), )
GENERATOR := -GNinja

# Ninja makes compilers disable colored output by default.
GENERATOR += -DVELOX_FORCE_COLORED_OUTPUT=ON
endif
endif

ifndef USE_CCACHE
ifneq ($(shell which ccache), )
USE_CCACHE=-DCMAKE_CXX_COMPILER_LAUNCHER=ccache
endif
endif

NUM_THREADS ?= $(shell getconf _NPROCESSORS_CONF 2>/dev/null || echo 1)
CPU_TARGET ?= "avx"

all: release			#: Build the release version

clean:					#: Delete all build artifacts
	rm -rf $(BUILD_BASE_DIR)

cmake:					#: Use CMake to create a Makefile build system
	mkdir -p $(BUILD_BASE_DIR)/$(BUILD_DIR) && \
	cmake -B \
		"$(BUILD_BASE_DIR)/$(BUILD_DIR)" \
		${CMAKE_FLAGS} \
		$(GENERATOR) \
		$(USE_CCACHE) \
		${EXTRA_CMAKE_FLAGS}

build:					#: Build the software based in BUILD_DIR and BUILD_TYPE variables
	cmake --build $(BUILD_BASE_DIR)/$(BUILD_DIR) -j $(NUM_THREADS)

debug:					#: Build with debugging symbols
	$(MAKE) cmake BUILD_DIR=debug BUILD_TYPE=Debug
	$(MAKE) build BUILD_DIR=debug -j ${NUM_THREADS}

release:				#: Build the release version
	$(MAKE) cmake BUILD_DIR=release BUILD_TYPE=Release && \
	$(MAKE) build BUILD_DIR=release

unittest: debug			#: Build with debugging and run unit tests
	cd $(BUILD_BASE_DIR)/debug && ctest -j ${NUM_THREADS} -VV --output-on-failure

help:					#: Show the help messages
	@cat $(firstword $(MAKEFILE_LIST)) | \
	awk '/^[-a-z]+:/' | \
	awk -F: '{ printf("%-20s   %s\n", $$1, $$NF) }'
