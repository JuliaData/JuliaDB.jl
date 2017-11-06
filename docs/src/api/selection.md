```@meta
CurrentModule = IndexedTables
```
# Selection

## Conventions

## Accessors

```@docs
columns
```
```@docs
rows
```
```@docs
keys(t::NextTable, args...)
```

```@docs
values(t::NextTable, args...)
```

```@docs
map(f, t::NextTable; kwargs...)
map(f, t::NDSparse; kwargs...)
```

```@docs
filter(f, t::NextTable)
filter(f, t::NDSparse)
```
