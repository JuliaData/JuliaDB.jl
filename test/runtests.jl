using JuliaDB
using Base.Test
using TextParse
using IndexedTables
using NamedTuples
using PooledArrays

include("test_util.jl")
include("test_query.jl")
include("test_join.jl")
include("test_misc.jl")
include("test_rechunk.jl")
include("test_readwrite.jl")
