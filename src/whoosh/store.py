#===============================================================================
# Copyright 2007 Matt Chaput
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


class LockError(Exception):
    pass


class Storage(object):
    """Abstract base class for storage objects.
    """
    
    def create_index(self, schema, indexname=None):
        raise NotImplementedError
    
    def open_index(self, indexname=None, schema=None):
        raise NotImplementedError
    
    def close(self):
        pass
    
    def optimize(self):
        pass







