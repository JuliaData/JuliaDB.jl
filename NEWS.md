## v0.7.0

- **(breaking)** `groupby` and `groupreduce` now select all but the grouped columns (as opposed to all columns) (#120)
- **(feature)** `usekey=true` keyword argument to `groupby` will cause the grouping function to be called with two arguments: the grouping key, and the selected subset of records. (#120)
- **(breaking)** leftjoin and outerjoin, operations don't speculatively create `DataValueArray` anymore. It will be created if there are some keys which do not have a corresponding match in the other table. (#121)
- **(feature)** `Not`, `Join`, `Between` and `Function` selectors have been added.
