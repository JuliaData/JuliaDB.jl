```@setup join
using JuliaDB
```

# Joins

## Table Joins

Table joins are accomplished through the [`join`](@ref) function.  

## Appending Tables with the Same Columns

The [`merge`](@ref) function will combine tables while maintaining the sorting of the 
primary key(s).

```@repl join
t1 = table(1:5, rand(5); pkey=1)
t2 = table(6:10, rand(5); pkey=1)
merge(t1, t2)
```