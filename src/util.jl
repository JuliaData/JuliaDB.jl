import IndexedTables: astuple

using NamedTuples

# re-export
export @NT

function tuplesetindex{N}(x::Tuple{Vararg{Any,N}}, v, i)
    ntuple(Val{N}) do j
        i == j ? v : x[j]
    end
end

@generated function tuplesetindex(x::NamedTuple, v, i::Symbol)
    fields = fieldnames(x)
    :(@NT($(fields...))(tuplesetindex(x, v, findfirst($fields, i))...))
end

@generated function tuplesetindex(x::NamedTuple, v, i::Int)
    fields = fieldnames(x)
    N = length(fields)
    quote
        tup = Base.@ntuple $N j -> i == j ? v : x[j]
        @NT($(fields...))(tuplesetindex(tup, v, i)...)
    end
end

function Base.isless(t1::NamedTuple, t2::NamedTuple)
    n1, n2 = length(t1), length(t2)
    for i = 1:min(n1, n2)
        a, b = t1[i], t2[i]
        if !isequal(a, b)
            return isless(a, b)
        end
    end
    return n1 < n2
end

function treereduce(f, xs, v0=xs[1])
    length(xs) == 0 && return v0
    length(xs) == 1 && return xs[1]
    l = length(xs)
    m = div(l, 2)
    f(treereduce(f, xs[1:m]), treereduce(f, xs[m+1:end]))
end


function subtable(nds, r)
    Table(nds.index[r], nds.data[r], presorted=true, copy=false)
end

function extrema_range{T}(x::AbstractArray{T}, r::UnitRange)
    if !(1 <= first(r) && last(r) <= length(x))
        throw(BoundsError(x, r))
    end

    isempty(r) && return extrema(x[r])
    mn = x[first(r)]
    mx = x[first(r)]
    @inbounds @simd for i in r
        mn = min(x[i], mn)
        mx = max(x[i], mx)
    end
    mn, mx
end

getbyheader(cols, header, i::Int) = cols[i]
getbyheader(cols, header, i::Symbol) = getcol(cols, header, string(i))
function getbyheader(cols, header, i::AbstractString)
    if !(i in header)
        throw(ArgumentError("Unknown column $i"))
    end
    getbyheader(cols, header, findfirst(header, i))
end

# pick the first of many columns
function getbyheader(cols, header, oneof::Tuple)
    for c in oneof
        try
            return getbyheader(cols, header, c)
        catch err
        end
    end
    throw(ArgumentError("Couldn't find any of the columns in $cs"))
end

function getbyheader_canonical(cols, header, oneof::Tuple)

    for c in oneof
        try
            getbyheader(cols, header, c)
            return first(oneof)
        catch err
        end
    end
    throw(ArgumentError("Couldn't find any of the columns in $oneof"))
end

function getbyheader_canonical(cols, header, oneof)
    getbyheader(cols, header, oneof)
end

"""
get a subset of vectors wrapped in Columns from a tuple of vectors
"""
function getcolsubset(cols, header, subcols)
    colnames = !isempty(header) ?
        vcat(map(i -> Symbol(getbyheader_canonical(header, header, i)), subcols)) :
        nothing

    if length(subcols) > 1
        Columns(map(i -> getbyheader(cols, header, i), subcols)...; names=colnames)
    else
        Columns(getbyheader(cols, header, subcols[1]); names=colnames)
    end
end

# Data loading utilities
using TextParse
using Glob

export @dateformat_str, load, csvread, loadTable, glob

"""
    loadTable(file::AbstractString;
              indexcols, datacols, agg, presorted, copy, kwargs...)

Load a CSV file into an Table data. `indexcols` (AbstractArray)
specifies which columns form the index of the data, `datacols`
(AbstractArray) specifies which columns are to be used as the data.
`agg`, `presorted`, `copy` options are passed on to `Table`
constructor, any other keyword argument is passed on to `readcsv`

Returns `data, implicit_index::Bool`, where the second value
indicates that a single implicit dimension with values 1:N
was added (e.g. if `indexcols=[]` is specified, or if the file
only has one column).
"""
function loadTable(file::AbstractString, delim=',';
                      indexcols=nothing,
                      datacols=nothing,
                      agg=nothing,
                      presorted=false,
                      copy=false,
                      csvread=TextParse.csvread,
                      kwargs...)

    #println("LOADING ", file)
    cols,header = csvread(file, delim; kwargs...)

    implicitindex = false

    if datacols === nothing
        if indexcols === nothing
            indexcols = 1:(length(cols)-1)
            datacols  = [length(cols)]
        else
            # all columns that aren't index
            _indexcols = map(i->getbyheader(1:length(cols), header, i), indexcols)
            datacols = [x for x in 1:length(cols) if !(x in _indexcols)]
        end
    elseif indexcols === nothing
        # all columns that aren't data
        _datacols = map(i->getbyheader(1:length(cols), header, i), datacols)
        indexcols = [x for x in 1:length(cols) if !(x in _datacols)]
    end

    if isempty(datacols)
        error("There must be at least one data column.")
    end

    data = getcolsubset(cols, header, datacols)

    if isempty(indexcols)
        # with no indexes, default to 1:n
        index = [1:length(data);]
        implicitindex = true
    else
        index = getcolsubset(cols, header, indexcols)
    end

    Table(index, data), implicitindex
end


using PooledArrays
using NullableArrays

"""
    MmappableArray

A convenience wrapper that can be used to pass around metadata about
an Mmapped array.

When serialized, this only writes the metadata and leaves out the data.

copy_mmap(io::IO, file::String, x::T) # =>
"""
type MmappableArray{T, N, A} <: AbstractArray{T, N}
    file::String
    offset::Int
    size::NTuple{N, Int}
    data::A
end

@inline Base.size(arr::MmappableArray) = arr.size
@inline Base.getindex(arr::MmappableArray, idx...) = arr.data[idx...]
if VERSION < v"0.6.0-dev"
    Base.linearindexing(arr::MmappableArray) = Base.linearindexing(arr.data)
else
    Base.IndexStyle{T,N,A}(::Type{MmappableArray{T,N,A}}) = Base.IndexStyle(A)
end

function Base.similar{M<:MmappableArray}(A::M, sz::Int...)
    # this is to keep Table constructor happy
    M("__unmmapped__", 0, sz, similar(A.data, sz...))
end

function Base.similar{T,R,N,M<:MmappableArray}(pa::PooledArray{T,R,N,M}, S::Type, dims::Dims)
    z = M("__unmmapped__", 0, dims, zeros(R, dims))
    PooledArray(PooledArrays.RefArray(z), S[])
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
function copy_mmap(io::IO, file::String, nds::Table)
    flush!(nds)
    index = copy_mmap(io, file, nds.index)
    data  = copy_mmap(io, file, nds.data)
    Table(index, data, copy=false, presorted=true)
end

function copy_mmap(file::String, data)
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

function unwrap_mmap(arr::Table)
    Table(unwrap_mmap(arr.index), unwrap_mmap(arr.data), presorted=true, copy=false)
end

unwrap_mmap(arr::AbstractArray) = arr


#### Fix serialization of NamedTuple types ####
immutable NTType end

function Base.serialize{NT<:NamedTuple}(io::AbstractSerializer, ::Type{NT})
    Base.serialize_type(io, NTType)
    if isa(NT, Union)
        serialize(io, Union)
        serialize(io, [NT.types...])
    else
        serialize(io, fieldnames(NT))
        serialize(io, NT.parameters)
    end
end

function Base.deserialize(io::AbstractSerializer, ::Type{NTType})
   fnames = deserialize(io)
   if fnames == Union
        types = deserialize(io)
        return Union{types...}
   else
       ftypes = deserialize(io)
       NT =  eval(:(NamedTuples.$(NamedTuples.create_tuple(fnames))))
       return NT{ftypes...}
   end
end

if VERSION < v"0.6.0-dev"
    function _repeated(x, n)
        repeated(x, n)
    end
else
    function _repeated(x, n)
        Iterators.repeated(x,n)
    end
end


import Dagger: approx_size

function approx_size(cs::Columns)
    sum(map(approx_size, astuple(cs.columns)))
end

function approx_size(t::IndexedTable)
    approx_size(t.data) + approx_size(t.index)
end

using PooledArrays

function approx_size(pa::PooledArray)
    approx_size(pa.refs) + approx_size(pa.pool)
end

# smarter merges on NullableArray + other arrays
import IndexedTables: promoted_similar

function promoted_similar(x::NullableArray, y::NullableArray, n)
    similar(x, promote_type(eltype(x),eltype(y)), n)
end

function promoted_similar(x::NullableArray, y::AbstractArray, n)
    similar(x, promote_type(eltype(x),eltype(y)), n)
end

function promoted_similar(x::AbstractArray, y::NullableArray, n)
    similar(y, promote_type(eltype(x),eltype(y)), n)
end

# The following is not inferable, this is OK because the only place we use
# this doesn't need it.
import Base: tuple_type_head, tuple_type_tail

function _map_params(f, T, S)
    (f(tuple_type_head(T), tuple_type_head(S)), _map_params(f, tuple_type_tail(T), tuple_type_tail(S))...)
end

_map_params(f, T::Type{Tuple{}},S::Type{Tuple{}}) = ()

map_params{T,S}(f, ::Type{T}, ::Type{S}) = f(T,S)

#function map_params{N}(f, T::Type{T} where T<:Tuple{Vararg{Any,N}}, S::Type{S} where S<: Tuple{Vararg{Any,N}})
Base.@pure function map_params{T<:Tuple,S<:Tuple}(f, ::Type{T}, ::Type{S})
    if nfields(T) != nfields(S)
        MethodError(map_params, (typeof(f), T,S))
    end
    Tuple{_map_params(f, T,S)...}
end

tuple_type_head{NT<: NamedTuple}(T::Type{NT}) = fieldtype(NT, 1)
Base.@pure function tuple_type_tail{NT<: NamedTuple}(T::Type{NT})
    Tuple{Base.argtail(NT.parameters...)...}
end

Base.@pure @generated function map_params{T<:NamedTuple,S<:NamedTuple}(f, ::Type{T}, ::Type{S})
    if fieldnames(T) != fieldnames(S)
        MethodError(map_params, (T,S))
    end
    NT = Expr(:macrocall, :(NamedTuples.$(Symbol("@NT"))), fieldnames(T)...)
    :($NT{_map_params(f, T, S)...})
end

