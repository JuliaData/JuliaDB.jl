using JuliaDB
import JuliaDB: DNextTable
export rechunk_together

function rechunk_together(left, right, lkey, rkey,
                          lselect, rselect; chunks=nworkers())
    # we will assume that right has to be aligned to left
    l = reindex(left, lkey, lselect)
    r = reindex(right, rkey, rselect)

    if has_overlaps(left.domains, true)
        l = rechunk(left, lkey, lselect, chunks=chunks)
    end

    splitters = map(last, l.domains)

    r = rechunk(right, rkey, rselect,
                splitters=splitters[1:end-1],
                chunks_presorted=true,
                affinities=map(x->first(Dagger.affinity(x))[1].pid, l.chunks),
               )
    l, r
end

function Base.join(f, left::DNextTable, right::DNextTable;
                   how=:inner,
                   lkey=pkeynames(left), rkey=pkeynames(right),
                   lselect=excludecols(left, lkey),
                   rselect=excludecols(right, rkey),
                   chunks=nworkers(),
                   kwargs...)

    l, r = rechunk_together(compute(left), compute(right),
                            lkey, rkey, rselect, lselect,
                            chunks=chunks)

    delayedmap(l.chunks, r.chunks) do x, y
        join(f, x, y, how=how, lkey=lkey, rkey=rkey,
             lselect=lselect, rselect=rselect, kwargs...)
    end |> fromchunks
end

function Base.join(left::DNextTable, right::DNextTable; kwargs...)
    join(IndexedTables.concat_tup, left, right; kwargs...)
end
