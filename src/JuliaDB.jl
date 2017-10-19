__precompile__()
module JuliaDB

using IndexedTables, Dagger, NamedTuples

import TextParse: csvread
import IndexedTables: Table
import Dagger: compute, distribute, free!, gather, load, save

# re-export
export IndexedTable, Columns

include("util.jl")
include("serialize.jl")
include("interval.jl")
include("dtable.jl")
include("sort.jl")

include("io.jl")
include("printing.jl")
include("dcolumns.jl")

include("indexing.jl")
include("query.jl")
include("join.jl")

include("diagnostics.jl")

end # module
