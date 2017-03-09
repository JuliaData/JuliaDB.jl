using PooledArrays
using NullableArrays

type MmappableArray{T, N, A} <: AbstractArray{T, N}
    file::String
    offset::Int
    size::NTuple{N, Int}
    data::A
end

@inline Base.size(arr::MmappableArray) = arr.size
@inline Base.getindex(arr::MmappableArray, idx...) = arr.data[idx...]
Base.linearindexing(arr::MmappableArray) = Base.linearindexing(arr.data)
Base.copy(mm::MmappableArray) = copy(mm.data) # it's no more useful to carry around the wrapper

function Base.similar{M<:MmappableArray}(A::M, sz::Int...)
    # this is to keep NDSparse constructor happy
    M("__unmmapped__", 0, sz, similar(A.data, sz...))
end

# construct an MmappableArray from a normal array, writing it to file
function MmappableArray{T}(io::IO, file::String, data::Array{T})
    if !isbits(T)
        error("Cannot Mmap non-bits array $T")
    end

    offset = position(io)
    arr = Mmap.mmap(io, typeof(data), size(data), offset)
    copy!(arr, data)
    Mmap.sync!(arr)
    seek(io, offset+sizeof(data))
    MmappableArray{T, ndims(data), typeof(data)}(file, offset, size(data), arr)
end

function MmappableArray(io::IO, file::String, data::PooledArray)
    mmaprefs = MmappableArray(io, file, data.refs)
    PooledArray(PooledArrays.RefArray(mmaprefs), data.pool)
end

function MmappableArray(file::String, data)
    open(file, "w+") do io
        MmappableArray(io, file, data)
    end
end

# Load an Mmap array from file
function MmappableArray{A}(file::String, ::Type{A}, offset::Int, dims)
    MmappableArray{eltype(A), ndims(A), A}(file, offset, dims, Mmap.mmap(file, A, dims, offset))
end

function Base.serialize(io::AbstractSerializer, arr::MmappableArray)
    Mmap.sync!(arr.data)
    Base.Serializer.serialize_type(io, typeof(arr))
    Base.serialize(io, (arr.file, arr.offset, arr.size))
    if arr.file == "__unmmapped__"
        Base.serialize(io, arr.data)
    end
end

function Base.deserialize{T, N, A}(io::AbstractSerializer, ::Type{MmappableArray{T,N,A}})
    (file, offset, dims)  = deserialize(io)
    if file == "__unmmapped__"
        data = deserialize(io)
        return MmappableArray{T,N,A}(file, offset, dims, data)
    end
    MmappableArray(file, A, offset, dims)
end

using IndexedTables

# assuming we don't use non-bits numbers
const MmappableTypes = Union{Integer, AbstractFloat, Complex, Char, DateTime, Date}

### Wrap arrays in Mmappable wrapper. Do this before serializing
copy_mmap(io, file, arr::PooledArray) = MmappableArray(io, file, arr)
copy_mmap{T<:MmappableTypes}(io, file, arr::Array{T}) = MmappableArray(io, file, arr)
function copy_mmap(io, file, arr::Columns)
    cs = map(x->copy_mmap(io, file, x), arr.columns)
    if all(x->isa(x, Int), fieldnames(cs))
        Columns(cs...)
    else
        # NamedTuple case
        Columns(cs...; names=fieldnames(cs))
    end
end
copy_mmap(io, file, arr::AbstractArray) = arr

## This must be called after sorting and aggregation!
function copy_mmap(io::IO, file::String, nds::NDSparse)
    flush!(nds)
    index = copy_mmap(io, file, nds.index)
    data  = copy_mmap(io, file, nds.data)
    NDSparse(index, data, copy=false, presorted=true)
end

function copy_mmap(file::String, data::NDSparse)
    open(file, "w+") do io
        copy_mmap(io, file, data)
    end
end

### Unwrap

# we should unwrap before processing because MmappableArray is
# not a complete array wrapper. We keep track of which arrays were
# part of which wrapper. If an array remains unchanged after it is
# deserialized by a proc, we can wrap it in MmappableArray before sending it
function unwrap_mmap(arr::MmappableArray)
    #track[arr.data] = (arr.file, arr.offset, arr.size)
    arr.data
end

function unwrap_mmap(arr::PooledArray)
    refs = unwrap_mmap(arr.refs)
    PooledArray(PooledArrays.RefArray(refs), arr.pool)
end

function unwrap_mmap(arr::Columns)
    cs = map(x->unwrap_mmap(x), arr.columns)
    if all(x->isa(x, Int), fieldnames(cs))
        Columns(cs...)
    else
        # NamedTuple case
        Columns(cs...; names=fieldnames(cs))
    end
end

function unwrap_mmap(arr::NDSparse)
    NDSparse(unwrap_mmap(arr.index), unwrap_mmap(arr.data), presorted=true, copy=false)
end

unwrap_mmap(arr::AbstractArray) = arr

using Base.Test
@testset "MmappableArray" begin
    X = rand(1000, 1000)
    f = tempname()
    M = MmappableArray(f, X)
    sf = tempname()
    open(io -> serialize(io, M), sf, "w")
    @test filesize(sf) < 1000 * 1000
    M2 = open(deserialize, sf)
    @test X == M
    @test M == M2

    P = PooledArray(rand(["A", "B"], 10^6))
    P1 = MmappableArray(f, P)
    psf = tempname()
    open(io -> serialize(io, P1), psf, "w")
    @test filesize(psf) < 10^5
    P2 = open(deserialize, psf)
    @test P2 == P1
    t = Int(now())
    T = map(DateTime, round(Int, linspace(t-10^6, t, 10^6)) |> collect)

    f = tempname()
    M = MmappableArray(f, T)
    sf = tempname()
    open(io -> serialize(io, M), sf, "w")
    @test filesize(sf) < 1000 * 1000
    @test open(deserialize, sf) == T

    nd = NDSparse(Columns(P, T), Columns(rand(10^6), rand(10^6)))
    ndf = tempname()
    mm = wrap_mmap(ndf, nd)
    ndsf = tempname()
    open(io -> serialize(io, mm), ndsf, "w")
    @test filesize(ndsf) < 10^6
    @test open(deserialize, ndsf) == nd
    nd2 = open(deserialize, ndsf)
    @test nd == unwrap_mmap(nd2)
    @test typeof(nd) == typeof(unwrap_mmap(nd2))
end

