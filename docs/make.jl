using Documenter, JuliaDB, IndexedTables

makedocs(
   modules = [JuliaDB, IndexedTables],
   clean = true,
   debug = true,
   format = Documenter.HTML(),
   sitename = "JuliaDB.jl",
   pages = [
        "index.md",
        "tutorial.md",
        "basics.md",
        # "aggregations.md",
        # "joins.md",
        "onlinestats.md",
        "plotting.md",
        # "out_of_core.md",
        "missing_values.md",
        "api.md",
   ]
)

deploydocs(
    repo = "github.com/JuliaComputing/JuliaDB.jl.git"
)
