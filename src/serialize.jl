using MemPool
import MemPool: mmwrite, mmread, MMSer

#### NullableArray

function mmwrite(io::AbstractSerializer, xs::NullableArray)
    Base.serialize_type(io, MMSer{NullableArray})
    
    mmwrite(io, BitArray(xs.isnull))
    mmwrite(io, xs.values)
end

function mmread(::Type{NullableArray}, io, mmap)
    isnull = deserialize(io)
    vals = deserialize(io)
    NullableArray(vals, isnull)
end

using PooledArrays

function mmwrite(io::AbstractSerializer, xs::PooledArray)
    Base.serialize_type(io, MMSer{PooledArray})
    
    mmwrite(io, xs.pool)
    mmwrite(io, xs.refs)
end

function mmread(::Type{PooledArray}, io, mmap)
    pool = deserialize(io)
    refs = deserialize(io)
    PooledArray(PooledArrays.RefArray(refs), pool)
end

# Columns, IndexedTable

function mmwrite(io::AbstractSerializer, xs::Columns)
    Base.serialize_type(io, MMSer{Columns})
    
    if eltype(xs) <: NamedTuple
        fnames = fieldnames(eltype(xs))
    else
        fnames = length(columns(xs))
    end

    serialize(io, fnames)
    for c in columns(xs)
        mmwrite(io, c)
    end
end

function mmread(::Type{Columns}, io, mmap)
    fnames = deserialize(io)
    if isa(fnames, Int)
        cols = [deserialize(io) for i=1:fnames]
        Columns(cols...)
    else
        cols = [deserialize(io) for i=1:length(fnames)]
        Columns(cols...; names=fnames)
    end
end

function mmwrite(io::AbstractSerializer, xs::IndexedTable)
    Base.serialize_type(io, MMSer{IndexedTable})

    flush!(xs)
    mmwrite(io, xs.index)
    mmwrite(io, xs.data)
end

function mmread(::Type{IndexedTable}, io, mmap)
    idx = deserialize(io)
    data = deserialize(io)
    IndexedTable(idx, data, presorted=true, copy=false)
end
