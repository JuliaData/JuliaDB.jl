using Documenter, JuliaDB, IndexedTables

makedocs(
   modules = [JuliaDB, IndexedTables],
   clean = true,
   debug = true,
   format = Documenter.HTML(),
   sitename = "JuliaDB.jl",
   pages = [
        "index.md",
        "basics.md",
        "tutorial.md",
        "onlinestats.md",
        "plotting.md",
        "missing_values.md",
        "api.md",
   ]
)

deploydocs(
    repo = "github.com/JuliaComputing/JuliaDB.jl.git"
)
