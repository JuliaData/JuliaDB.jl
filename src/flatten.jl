import IndexedTables: flatten

function mapslices(f, x::DNDSparse, dims; name=nothing)
    iterdims = setdiff([1:ndims(x);], map(d->keyindex(x, d), dims))
    if iterdims != [1:length(iterdims);]
        throw(ArgumentError("$dims must be the trailing dimensions of the table. You can use `permutedims` first to permute the dimensions."))
    end

    # Note: the key doesn't need to be put in a tuple, this is
    # also bad for sortperm, but is required since DArrays aren't
    # parameterized by the container type Columns
    vals = isempty(dims) ?  values(x) : (keys(x, (dims...)), values(x))
    tmp = ndsparse((keys(x, (iterdims...)),), vals,
                   allowoverlap=false, closed=true)

    cs = delayedmap(tmp.chunks) do c
        ks = isempty(dims) ? columns(columns(keys(c))[1]) : IndexedTables.concat_cols(columns(keys(c))[1], columns(values(c))[1])
        vs = isempty(dims) ? columns(values(c)) : columns(values(c))[2]
        y = ndsparse(ks, vs)
        mapslices(f, y, dims; name=name)
    end
    fromchunks(cs)
  # cache_thunks(mapchunks(y -> mapslices(f, y, dims, name=name),
  #                        t, keeplengths=false))
end

mapslices(f, x::DNDSparse, dims::Symbol; name=nothing) =
    mapslices(f, x, (dims,); name=name)

function flatten(x::DNextTable, col)
    fromchunks(delayedmap(t -> flatten(t, col), x.chunks))
end
