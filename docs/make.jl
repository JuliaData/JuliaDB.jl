using Documenter, JuliaDB, IndexedTables, Dagger, TextParse, Glob

load_dir(x) = map(file -> joinpath("lib", x, file), readdir(joinpath(Base.source_dir(), "src", "lib", x)))

makedocs(
   modules = [JuliaDB],
   clean = false,
   format = [:html, :latex],
   sitename = "JuliaDB",
   pages = ["index.md"],
   pages = Any[
       "Introduction" => "index.md",
       "Tutorial" => "tutorial.md",
       "API Reference" => "apireference.md"
   ],
   assets = ["assets/jc.css"]
)
