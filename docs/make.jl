using Documenter, JuliaDB, Dagger, TextParse, Glob

#makedocs(format=:html, sitename="JuliaDB",
#         pages=["index.md"])

load_dir(x) = map(file -> joinpath("lib", x, file), readdir(joinpath(Base.source_dir(), "src", "lib", x)))

makedocs(
   modules = [JuliaDB],
   clean = false,
   format = [:html, :latex],
   sitename = "JuliaDB",
   pages = ["index.md"],
   assets = ["assets/jc.css"]
)
