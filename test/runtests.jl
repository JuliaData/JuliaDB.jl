using JuliaDB
using Base.Test
using TextParse
using IndexedTables
using NamedTuples
using PooledArrays

import JuliaDB: MmappableArray, copy_mmap, unwrap_mmap, tuplesetindex

include("test_util.jl")
include("test_query.jl")
include("test_join.jl")
include("test_misc.jl")
#include("test_readwrite.jl")
