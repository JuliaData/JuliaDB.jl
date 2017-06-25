using OnlineStatsBase

import OnlineStatsBase: merge, Series, OnlineStat

export aggregate_stats

function Series(t::JuliaDB.DTable, stats::OnlineStat...)
   chunk_aggs = map(delayed(x->Series(x.data, map(copy, stats)...)), t.chunks)
   collect(treereduce(delayed(merge), chunk_aggs))
end

function Series{K,V}(t::JuliaDB.DTable{K,V}, stats::OnlineStat{(1,0), 1}...; xcols=1:(nfields(V)-1), ycol=nfields(V))
    @assert !isempty(xcols) && nfields(V) != 0

    getindex.([[1,2,3], [4,5,6]]', 1:3)

    function inner_series(chunk)
        xmatrix = getindex.([chunk.data.columns[xcols]...]', 1:length(chunk))
        y = chunk.data.columns[ycol]
        Series(xmatrix, y, map(copy, stats)...)
    end

    chunk_aggs = map(delayed(inner_series), t.chunks)
    collect(treereduce(delayed(merge), chunk_aggs))
end

function aggregate_stats(t::IndexedTable, series::Series)
    src_idxs = t.index
    src_data = t.data
    dest_idxs = similar(src_idxs,0)
    dest_data = fill(series,0)
    n = length(src_idxs)
    i1 = 1
    while i1 <= n
        val = copy(series)
        i = i1+1
        @inbounds while i <= n && IndexedTables.roweq(src_idxs, i, i1) 
            fit!(val, src_data[i])
            i += 1
        end 
        push!(dest_idxs, src_idxs[i1])
        push!(dest_data, val)
        i1 = i
    end 
    IndexedTable(dest_idxs, dest_data, presorted=true)
end

function aggregate_stats(t::JuliaDB.DTable, series::Series)
    t1 = mapchunks(c->aggregate_stats(c, copy(series)), t, keeplengths=false)
    if JuliaDB.has_overlaps(t1.subdomains, true)
        overlap_merge = (x, y) -> merge(x, y, agg=merge)
        t2 = JuliaDB.rechunk(t1, merge=(ts...) -> JuliaDB._merge(overlap_merge, ts...), closed=true)
        cache_thunks(t2)
    else
        cache_thunks(t1)
    end
end

function aggregate_stats(t::Union{DTable, IndexedTable}, stats...)
    aggregate_stats(t, Series(stats...))
end
