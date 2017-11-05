using Documenter, JuliaDB, IndexedTables, Dagger, TextParse, Glob

load_dir(x) = map(file -> joinpath("lib", x, file), readdir(joinpath(Base.source_dir(), "src", "lib", x)))

makedocs(
   modules = [JuliaDB],
   clean = false,
   format = [:html],#, :latex],
   sitename = "JuliaDB",
   pages = Any[
       "Home" => "index.md",
       "API Reference" => "api/index.md",
       "Data Structures" => "api/datastructures.md",
       "Selection" => "api/selection.md",
       "Column manipulation" => "api/coldict.md",
       "Aggregation" => "api/aggregation.md",
       "Joins" => "api/joins.md",
       "Loading and Saving" => "api/io.md",
   ],
   assets = ["assets/custom.css"]
)

deploydocs(
    repo = "github.com/JuliaComputing/JuliaDB.jl.git",
    target = "build",
    julia = "0.6",
    osname = "linux",
    deps = nothing,
    make = nothing,
)
