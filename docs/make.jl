using Documenter, JuliaDB, IndexedTables

makedocs(
   modules = [JuliaDB, IndexedTables],
   clean = true,
   debug = true,
   format = [:html],
   sitename = "JuliaDB.jl",
   pages = [
        "index.md",
        "loading_and_saving.md",
        "data_structures.md",
        "selection.md",
        "aggregations.md",
        "joins.md",
        "onlinestats_integration.md",
        "plotting.md",
        "out_of_core.md",
        "tutorial.md",
        "api.md",
   ]
)

deploydocs(
    repo = "github.com/JuliaComputing/JuliaDB.jl.git",
)
