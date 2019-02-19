#-----------------------------------------------------------------------# DataValueArray

function mmwrite(io::AbstractSerializer, xs::DataValueArray)
    Serialization.serialize_type(io, MMSer{DataValueArray})
    
    mmwrite(io, BitArray(xs.isna))
    mmwrite(io, xs.values)
end

function mmread(::Type{DataValueArray}, io, mmap)
    isnull = deserialize(io)
    vals = deserialize(io)
    DataValueArray(vals, isnull)
end

#-----------------------------------------------------------------------# PooledArray
function mmwrite(io::AbstractSerializer, xs::PooledArray)
    Serialization.serialize_type(io, MMSer{PooledArray})
    
    mmwrite(io, xs.refs)
    mmwrite(io, xs.invpool)
end

function mmread(::Type{PooledArray}, io, mmap)
    refs = deserialize(io)
    invpool = deserialize(io)
    PooledArray(PooledArrays.RefArray(refs), invpool)
end

#-----------------------------------------------------------------------# Columns

function mmwrite(io::AbstractSerializer, xs::Columns)
    Serialization.serialize_type(io, MMSer{Columns})
    
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
        Columns(Tuple(cols))
    else
        cols = [deserialize(io) for i=1:length(fnames)]
        Columns(Tuple(cols); names=fnames)
    end
end

#-----------------------------------------------------------------------# NDSparse
function mmwrite(io::AbstractSerializer, xs::NDSparse)
    Serialization.serialize_type(io, MMSer{NDSparse})

    flush!(xs)
    mmwrite(io, xs.index)
    mmwrite(io, xs.data)
end

function mmread(::Type{NDSparse}, io, mmap)
    idx = deserialize(io)
    data = deserialize(io)
    NDSparse(idx, data, presorted=true, copy=false)
end

#-----------------------------------------------------------------------# IndexedTable
function mmwrite(io::AbstractSerializer, xs::IndexedTable)
    Serialization.serialize_type(io, MMSer{IndexedTable})

    #flush!(xs)
    mmwrite(io, rows(xs))
    mmwrite(io, xs.pkey)
    mmwrite(io, xs.perms)
end

function mmread(::Type{IndexedTable}, io, mmap)
    data = deserialize(io)
    pkey = deserialize(io)
    perms = deserialize(io)
    table(data, pkey=pkey, perms=perms, presorted=true, copy=false)
end
