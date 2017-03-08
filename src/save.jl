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

_mmappable(io, file, arr::PooledArray) = MmappableArray(io, file, arr)
_mmappable{T<:MmappableTypes}(io, file, arr::Array{T}) = MmappableArray(io, file, arr)
function _mmappable(io, file, arr::Columns)
    cs = map(x->_mmappable(io, file, x), arr.columns)
    if all(x->isa(x, Int), fieldnames(cs))
        Columns(cs...)
    else
        # NamedTuple case
        Columns(cs...; names=fieldnames(cs))
    end
end
_mmappable(io, file, arr::AbstractArray) = arr

## This must be called after sorting and aggregation!
function MmappableNDSparse(io::IO, file::String, nds::NDSparse)
    index = _mmappable(io, file, nds.index)
    data  = _mmappable(io, file, nds.data)
    NDSparse(index, data, copy=false, presorted=true)
end

function MmappableNDSparse(file::String, data::NDSparse)
    flush!(data)
    open(file, "w+") do io
        MmappableNDSparse(io, file, data)
    end
end

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
    mm = MmappableNDSparse(ndf, nd)
    ndsf = tempname()
    open(io -> serialize(io, mm), ndsf, "w")
    @test filesize(ndsf) < 10^6
    @test open(deserialize, ndsf) == nd
end
