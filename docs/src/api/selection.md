```@meta
CurrentModule = IndexedTables
DocTestSetup = quote
    using JuliaDB
end
```
# Selection

## Select

This section deals with the ways of selecting a column or a subset of columns.

```@index
```

```@docs
select
```

```@docs
columns(t)
```

```@docs
columns(t, selection)
```

```@docs
rows(t)
```

```@docs
rows(t, selection)
```

## Map

```@docs
map(f, t::NextTable; kwargs...)
```

```@docs
map(f, t::NDSparse; kwargs...)
```
## Filter

```@docs
filter(f, t::NextTable)
```

```@docs
filter(f, t::NDSparse)
```
