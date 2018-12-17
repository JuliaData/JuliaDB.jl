__precompile__()
module JuliaDB

using IndexedTables, Dagger, OnlineStats, Distributed, Serialization, Nullables, Printf, Statistics

import Base: collect, join
import IndexedTables: IndexedTable, table, NDSparse, ndsparse, Tup, groupjoin
import TextParse: csvread
import Dagger: compute, distribute, load, save
using DataValues

# re-export
export AbstractNDSparse, NDSparse, IndexedTable, Columns, colnames,
       table, ndsparse, compute, groupby, summarize, groupreduce, groupjoin,
       ColDict, insertafter!, insertbefore!, @cols, setcol, pushcol,
       popcol, insertcol, insertcolafter, insertcolbefore,
       renamecol, NA, dropna, flatten, ML, All, Not, Between, Keys

include("util.jl")
include("serialize.jl")
include("interval.jl")
include("table.jl")
include("ndsparse.jl")
include("reshape.jl")

# equality

function (==)(x::DDataset, y::Union{Dataset, DDataset})
    y1 = distribute(y, length.(domainchunks(rows(x))))
    res = delayed(==, get_result=true).(x.chunks, y1.chunks)
    all(collect(delayed((xs...) -> [xs...])(res...)))
end
function (==)(x::DDataset, y::Dataset)
    collect(x) == y
end
(==)(x::Dataset, y::DDataset) = y == x

include("iteration.jl")
include("sort.jl")

include("io.jl")
include("printing.jl")

include("indexing.jl")
include("selection.jl")
include("reduce.jl")
include("flatten.jl")
include("join.jl")

include("diagnostics.jl")
include("recipes.jl")
include("ml.jl")

end # module
