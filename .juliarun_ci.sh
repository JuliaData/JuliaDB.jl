#!/bin/sh

julia << EOF

Pkg.clone("https://github.com/shashi/TextParse.jl.git")
Pkg.checkout("PooledArrays")
Pkg.checkout("PooledArrays", "s/abstractarray-refs")
Pkg.clone("https://github.com/JuliaComputing/IndexedTables.jl.git")
Pkg.clone("https://github.com/JuliaParallel/Dagger.jl.git")

EOF
