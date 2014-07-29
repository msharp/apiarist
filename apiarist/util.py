# Copyright 2014 Max Sharples
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utility functions that have no external dependencies."""

import sys
import logging


class NullHandler(logging.Handler):
    def emit(self, record):
        pass


def log_to_null(name=None):
    """Set up a null handler for the given stream, to suppress
    no handlers could be found" warnings."""
    logger = logging.getLogger(name)
    logger.addHandler(NullHandler())


def log_to_stream(name=None, stream=None, format=None, level=None,
                  debug=False):
    """Set up logging."""
    if level is None:
        level = logging.DEBUG if debug else logging.INFO

    if format is None:
        format = '%(message)s'

    if stream is None:
        stream = sys.stderr

    handler = logging.StreamHandler(stream)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(format))

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
