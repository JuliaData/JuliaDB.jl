_merger(f) = f
_merger(f::OnlineStat) = merge
_merger(f::Tup) = map(_merger, f)

function reduce(f, t::DDataset; dims = nothing, cache = false, kw...)
    if dims !== nothing
        if !isa(t, DNDSparse)
            error("`dims` only supported for NDSparse")
        end
        if !isempty(kw)
            error("unsupported keyword arguments $kw")
        end
        return _reducedim(f, t, dims, cache)
    end
    if haskey(kw, :init)
        return reduce_inited(f, t, kw.init, get(kw, :select, nothing))
    end
    select = get(kw, :select, valuenames(t))
    xs = delayedmap(t.chunks) do x
        f = isa(f, OnlineStat) ? copy(f) : f # required for > 1 chunks on the same proc
        reduce(f, x; select=select)
    end
    if f isa Tup
        g, _ = IndexedTables.init_funcs(f, false)
    else
        g = f
    end
    h = (a,b)->IndexedTables._apply(_merger(g), a,b)
    collect(get_context(), treereduce(delayed(h), xs))
end

function reduce_inited(f, t::DDataset, v0, select)
    xs = delayedmap(t.chunks) do x
        f = isa(f, OnlineStat) ? copy(f) : f
        reduce(f, x; select=select === nothing ? rows(x) : select)
    end
    g = isa(f, OnlineStat) ? merge : f
    merge(collect(get_context(), treereduce(delayed(g), xs)), v0)
end

function groupreduce(f, t::DDataset, by=pkeynames(t); kwargs...)
    @noinline function groupchunk(x)
        groupreduce(f, x, by; kwargs...)
    end
    if f isa Tup || t isa DIndexedTable
        g, _ = IndexedTables.init_funcs(f, false)
    else
        g = f
    end
    h = _merger(g)
    if (f isa ApplyColwise) && f.functions isa Union{Function, Type}
        mergef = (x,y) -> map(f.functions, x,y)
    else
        mergef = (x,y) -> IndexedTables._apply(h, x,y)
    end
    @noinline function mergechunk(x, y)
        # use NDSparse's merge
        if x isa IndexedTable
            z = merge(_convert(NDSparse, x), _convert(NDSparse, y), agg=mergef)
            _convert(IndexedTable, z)
        else
            merge(x,y, agg=mergef)
        end
    end

    t1 = fromchunks(delayedmap(groupchunk, t.chunks))
    with_overlaps(t1, true) do cs
        treereduce(delayed(mergechunk), cs)
    end
end

function groupby(f, t::DDataset, by=pkeynames(t);
                 select=(t isa DNDSparse ? valuenames(t) : Not(by)),
                 kwargs...)
    by = lowerselection(t, by)
    select = lowerselection(t, select)
    if (by isa Tup) && isempty(by)
        @noinline function _groupby(x)
            groupby(f, x, by;  select=select, kwargs...)
        end
        collect(get_context(), delayed(_groupby)(
            treereduce(delayed(_merge), t.chunks)
            )
       )
   elseif by != lowerselection(t, Keys()) || has_overlaps(t.domains, closed=true)
       subsel = Tuple(setdiff(select, by))
       # translate the new selection
       t = rechunk(t, by, subsel)
       newselect = map(select) do x
           if x in by
               findall(in(x), by)[1]
           else
               x1 = findall(in(x), subsel)
               x1[1] + length(by)
           end
       end
       return groupby(f, t; select=newselect, kwargs...)
    else
        @noinline function groupchunk(x)
            groupby(f, x, by; select=select, kwargs...)
        end

        return fromchunks(delayedmap(groupchunk, t.chunks))
    end
end

function _reducedim(f, x::DNDSparse, dims, cache)
    keep = setdiff([1:ndims(x);], dims) # TODO: Allow symbols
    if isempty(keep)
        throw(ArgumentError("to remove all dimensions, use `reduce(f, A)`"))
    end
    groupreduce(f, x, (keep...,), select=valuenames(x), cache=cache)
end

_reducedim(f, x::DNDSparse, dims::Symbol, cache) = _reducedim(f, x, [dims], cache)

"""
    reducedim_vec(f::Function, t::DNDSparse, dims)

Like `reducedim`, except uses a function mapping a vector of values to a scalar instead of a 2-argument scalar function.

See also [`reducedim`](@ref) and [`aggregate_vec`](@ref).
"""
function reducedim_vec(f, x::DNDSparse, dims)
    keep = setdiff([1:ndims(x);], dims)
    if isempty(keep)
        throw(ArgumentError("to remove all dimensions, use `reduce(f, A)`"))
    end

    t = selectkeys(x, (keep...,); agg=nothing)
    cache_thunks(groupby(f, t))
end

reducedim_vec(f, x::DNDSparse, dims::Symbol) = reducedim_vec(f, x, [dims])
Base.@deprecate aggregate_stats(s, t; by=pkeynames(t), with=valuenames(t)) groupreduce(s, t, by; select=with)
