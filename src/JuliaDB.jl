__precompile__()
module JuliaDB

using IndexedTables, Dagger, NamedTuples, OnlineStatsBase

import Base: collect, select, join
import IndexedTables: NextTable, table, NDSparse, ndsparse, Tup
import TextParse: csvread
import IndexedTables: Table
import Dagger: compute, distribute, free!, gather, load, save
using DataValues

# re-export
export IndexedTable, NDSparse, NextTable, Columns, colnames,
       table, ndsparse, compute, groupby, groupreduce,
       ColDict, insertafter!, insertbefore!, @cols, setcol, pushcol,
       popcol, insertcol, insertcolafter, insertcolbefore, permutecols, renamecol, NA, dropna

include("util.jl")
include("serialize.jl")
include("interval.jl")
include("table/table.jl")
include("dndsparse.jl")
include("table/groupby.jl")
include("iteration.jl")
include("sort.jl")

include("io.jl")
include("printing.jl")

include("indexing.jl")
include("query.jl")
include("join.jl")

include("diagnostics.jl")

end # module
