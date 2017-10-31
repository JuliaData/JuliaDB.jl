using OnlineStatsBase
using StatsBase

import OnlineStatsBase: Series, merge, OnlineStat

export aggregate_stats, Series

"""
`Series(xs::DArray, stats::OnlineStat...)`

Create an `OnlineStats.Series` object with some initial data.

## Example:

    julia> t = IndexedTable(Columns([1,1,1,2,2,2,3,3,3], [1,2,3,1,2,3,1,2,3]),
                            0.5,1,1.5,1,2,3,2,3,4]) |> distribute

    julia> Series(column(t, 1), Mean())
    ▦ Series{0,Tuple{Mean},EqualWeight}
    ┣━━ EqualWeight(nobs = 9)
    ┗━━━┓
        ┗━━ Mean(2.0)
"""
function Series(xs::DArray, stats::OnlineStat...)
    function inner_series(xc)
        Series(xc, map(copy, stats)...)
    end

    chunk_aggs = map(delayed(inner_series), xs.chunks)
    collect(treereduce(delayed(merge), chunk_aggs))
end

"""
`Series(xs::Tuple{DArray, DArray}, stats::OnlineStat...)`

Create an `OnlineStats.Series` object with some initial data. Matrix and vector
inputs to Stats like LinReg (linear regression) should be passed in as a tuple.

## Example:

    julia> t = IndexedTable(Columns([1,1,1,2,2,2,3,3,3], [1,2,3,1,2,3,1,2,3]),
                            0.5,1,1.5,1,2,3,2,3,4]) |> distribute
    julia> reg = Series((keys(t), values(t)), LinReg(2))
    ▦ Series{(1, 0),Tuple{LinReg},EqualWeight}
    ┣━━ EqualWeight(nobs = 9)
    ┗━━━┓
        ┗━━ LinReg: β(0.0) = [0.615385 0.448718]
"""
function Series(inp::Tuple{<:DArray, <:DArray}, stats::OnlineStat...)
    xs, ys = inp
    function inner_series(xc, yc)
        Series((xc, yc), map(copy, stats)...)
    end

    chunk_aggs = map(delayed(inner_series), xs.chunks, ys.chunks)
    collect(treereduce(delayed(merge), chunk_aggs))
end


"""
`Series(xs::DTable, stats::OnlineStat...)`

Create an `OnlineStats.Series` object with some initial data.
Data is taken to be `values(t)` by default.

# Example

    julia> Series(t, Mean())
    ▦ Series{0,Tuple{Mean},EqualWeight}
    ┣━━ EqualWeight(nobs = 9)
    ┗━━━┓
        ┗━━ Mean(2.0)
"""
function Series(t::Union{DTable,IndexedTable}, stats::OnlineStat...)
    Series(values(t), stats...)
end

# spcialization for linear regression
function Series(inp::Tuple{<:Columns, AbstractVector}, stats::OnlineStat{(1,0)}...)
    x, y = inp
    xmatrix = getindex.([columns(x)...]', 1:length(x))

    Series((xmatrix, y), stats...)
end

"""
`aggregate_stats(series::Series, t::Union{IndexedTable, DTable; by, with)`

Aggregate common indices with an `OnlineStas.Series` object.

Computes the given Online stat for every group of values with equal indices.

    julia> t = IndexedTable(Columns([1,1,1,2,2,2,3,3,3],
                                    [1,2,3,1,2,3,1,2,3]),
                            [0.5,1,1.5,1,2,3,2,3,4]) |> distribute

    # keep only first dimension, aggregate equal indices
    julia> means = aggregate_stats(Series(Mean()), select(t,1))
    ──┬────────────────────────────────────
    1 │ ▦ Series{0,Tuple{Mean},EqualWeight}
    ┣━━ EqualWeight(nobs = 3)
    ┗━━━┓
        ┗━━ Mean(1.0)
    2 │ ▦ Series{0,Tuple{Mean},EqualWeight}
    ┣━━ EqualWeight(nobs = 3)
    ┗━━━┓
        ┗━━ Mean(2.0)
    3 │ ▦ Series{0,Tuple{Mean},EqualWeight}
    ┣━━ EqualWeight(nobs = 3)
    ┗━━━┓
        ┗━━ Mean(3.0)
"""
function aggregate_stats(series::Series, t::Union{IndexedTable, DTable}; by=keyselector(t), with=valueselector(t))
    aggregate_stats(series, rows(t, by), rows(t, with))
end

@inline function _fit!(series, xs::Tup, y...)
    fit!(series, ([xs...], y...))
end

@inline function _fit!(series, args...)
    fit!(series, args...)
end

function aggregate_stats(series::Series, ks::AbstractVector, vs::AbstractVector...)
    dest_idxs = similar(ks,0)
    dest_data = fill(series,0)
    n = length(ks)
    i1 = 1
    while i1 <= n
        val = copy(series)
        i = i1
        @inbounds while i <= n && (isa(ks, Columns) ? IndexedTables.roweq(ks, i, i1) : ks[i] == ks[i1])
            _fit!(val, map(v->v[i], vs)...)
            i += 1
        end
        push!(dest_idxs, ks[i1])
        push!(dest_data, val)
        i1 = i
    end
    IndexedTable(dest_idxs, dest_data, presorted=true)
end

"""
`aggregate_stats(series::Series, ks::AbstractVector, vs::AbstractVector...)`

Compute the online stat (`series`) for every group of indices in `vs` for which the values in `ks` are equal.

    julia> t = IndexedTable(Columns([1,1,1,2,2,2,3,3,3],
                                    [1,2,3,1,2,3,1,2,3]),
                            [0.5,1,1.5,1,2,3,2,3,4]) |> distribute

    julia> regs = aggregate_stats(Series(LinReg(2)), keys(t, 1), keys(t), values(t))
    ──┬───────────────────────────────────────────
    1 │ ▦ Series{(1, 0),Tuple{LinReg},EqualWeight}
    ┣━━ EqualWeight(nobs = 3)
    ┗━━━┓
        ┗━━ LinReg: β(0.0) = [1.55431e-15 0.5]
    2 │ ▦ Series{(1, 0),Tuple{LinReg},EqualWeight}
    ┣━━ EqualWeight(nobs = 3)
    ┗━━━┓
        ┗━━ LinReg: β(0.0) = [1.55431e-15 1.0]
    3 │ ▦ Series{(1, 0),Tuple{LinReg},EqualWeight}
    ┣━━ EqualWeight(nobs = 3)
    ┗━━━┓
        ┗━━ LinReg: β(0.0) = [0.333333 1.0]


"""
function aggregate_stats(series::Series, ks::DArray, vs::DArray...)
    agg_chunk = delayed() do kchunk, vchunks...
        aggregate_stats(copy(series), kchunk, vchunks...)
    end
    out_chunks = map(agg_chunk, ks.chunks, map(x->x.chunks, vs)...)
    chunks = compute(get_context(), delayed(vcat; meta=true)(out_chunks...))
    t = fromchunks(chunks, allowoverlap=true)
    if JuliaDB.has_overlaps(t.subdomains, true)
        overlap_merge = (x, y) -> merge(x, y, agg=merge)
        t1 = with_overlaps(t, true) do cs
            treereduce(delayed(overlap_merge), cs)
        end
        cache_thunks(t1)
    else
        cache_thunks(t)
    end
end
