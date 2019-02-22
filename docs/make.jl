using Documenter, JuliaDB, IndexedTables

@info "makedocs"
makedocs(
   clean = true,
   debug = true,
   format = Documenter.HTML(),
   sitename = "JuliaDB.jl",
   pages = [
        "index.md",
        "basics.md",
        "operations.md",
        "joins.md",
        "onlinestats.md",
        "plotting.md",
        "missing_values.md",
        "out_of_core.md",
        "ml.md",
        "tutorial.md",
        "api.md",
   ]
)

@info "deploydocs"
deploydocs(
    repo = "github.com/JuliaComputing/JuliaDB.jl.git"
)
