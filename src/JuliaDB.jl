module JuliaDB

using IndexedTables, Dagger, NamedTuples

# re-export
export NDSparse, Columns

include("util.jl")
include("interval.jl")
include("dtable.jl")
include("load.jl")
include("load-save.jl")
include("printing.jl")
include("dcolumns.jl")

include("indexing.jl")
include("query.jl")

end # module
