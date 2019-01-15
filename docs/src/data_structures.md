```@setup data_structures
using JuliaDB
```

# Data Structures

JuliaDB offers two main data structures as well as distributed counterparts.

## [`IndexedTable`](@ref)

An [`IndexedTable`](@ref) is wrapper around a (named) tuple of Vectors, but it behaves like
a Vector of (named) tuples.

```@example data_structures
x = 1:10
y = 'a':'j'
z = randn(10)
t = table((x=x, y=y, z=z); pkey = [:x, :y])
t[1]
t[end]
```


## [`NDSparse`](@ref)

An [`NDSparse`](@ref) has a similar underlying structure to [`IndexedTable`](@ref), but it
behaves like a sparse array with arbitrary indices.

```@example data_structures
nd = ndsparse((x=x, y=y), (z=z,))
nd[1, 'a']
nd[10, 'j'].z
nd[end]
```