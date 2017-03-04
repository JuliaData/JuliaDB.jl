module JuliaDB

using IndexedTables, Dagger, NamedTuples

# re-export
export NDSparse

include("util.jl")
include("interval.jl")
include("dtable.jl")
include("load.jl")
include("printing.jl")

include("indexing.jl")
include("query.jl")

end # module
