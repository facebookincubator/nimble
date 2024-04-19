#!/bin/bash
# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

$FLATC -j -o "${OUT}" --filename-suffix Generated ./Footer.fbs
