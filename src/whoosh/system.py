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


from struct import Struct, calcsize

_INT_SIZE = calcsize("!i")
_SHORT_SIZE = calcsize("!H")
_LONG_SIZE = calcsize("!Q")
_FLOAT_SIZE = calcsize("!f")

_sbyte_struct = Struct("!b")
_ushort_struct = Struct("!H")
_int_struct = Struct("!i")
_uint_struct = Struct("!I")
_ulong_struct = Struct("!Q")
_float_struct = Struct("!f")

pack_sbyte = _sbyte_struct.pack
pack_ushort = _ushort_struct.pack
pack_int = _int_struct.pack
pack_uint = _uint_struct.pack
pack_ulong = _ulong_struct.pack
pack_float = _float_struct.pack

unpack_sbyte = _sbyte_struct.unpack
unpack_ushort = _ushort_struct.unpack
unpack_int = _int_struct.unpack
unpack_uint = _uint_struct.unpack
unpack_ulong = _ulong_struct.unpack
unpack_float = _float_struct.unpack

