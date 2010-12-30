#===============================================================================
# Copyright 2010 Matt Chaput
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

"""
This module contains common utility objects/functions for the other query
parser modules.
"""

import re


class QueryParserError(Exception):
    def __init__(self, cause, msg=None):
        super(QueryParserError, self).__init__(str(cause))
        self.cause = cause


def rcompile(pattern, flags=0):
    if not isinstance(pattern, basestring):
        # If it's not a string, assume it's already a compiled pattern
        return pattern
    return re.compile(pattern, re.UNICODE | flags)


def get_single_text(field, text, **kwargs):
    # Just take the first token
    for t in field.process_text(text, mode="query", **kwargs):
        return t


