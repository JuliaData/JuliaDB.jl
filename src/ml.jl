# Not correct for `map(pick(:foo), t)`
Base.names(t::DTable) = fieldnames(eltype(t))
Base.names(t::IndexedTable) = fieldnames(eltype(t))

# Core schema types

width(::Void) = 0
numeric(::Void, x) = []

struct Continuous{T}
  μ::T
  σ::T
end

width(::Continuous) = 1
numeric(c::Continuous, x) = (x - c.μ) / c.σ

struct Categorical{T}
  items::Vector{T}
end

width(c::Categorical) = length(c.items)
numeric(c::Categorical, x) = x .== c.items

struct Maybe{T}
  feature::T
end

width(c::Maybe) = width(c.feature) + 1
numeric(c::Maybe, x) =
  DataValues.isna(x) ?
    [0, zeros(width(c.feature))...] :
    [1, numeric(c.feature, get(x))...]

# Schema inference

const Schema = Dict{Symbol,Any}

schema(t) = Schema(col => schema(column(t, col)) for col in names(t))

width(sch::Schema) = sum(width(s) for s in values(sch))

schema(::AbstractVector) = nothing

schema(xs::AbstractVector{T}) where T <: Real =
  Continuous{Float64}(mean(xs), std(xs))

schema(xs::PooledArray{T}) where T = Categorical{T}(unique(xs))

schema(xs::AbstractVector{<:DataValue}) = Maybe(schema(dropna(xs)))

# Can't use dropna
schema(xs::DArray{<:DataValue{<:Real}}) = Maybe(Continuous(mean(xs), std(xs)))

splitschema(xs::Schema, ks...) =
  filter((k,v) -> k ∉ ks, xs),
  filter((k,v) -> k ∈ ks, xs)

# Dataset construction

tovec(sch::Schema, row::NamedTuple) =
  Float32.(vcat(map(col -> numeric(get(sch, col, nothing), getfield(row, col)), fieldnames(row))...))

tomat(sch::Schema, data) =
  reduce(hcat, map(r -> tovec(sch, r), data))

# Summary stats

using OnlineStats

Base.mean(xs::DArray{<:DataValue}) =
  Dagger.reduceblock(x->Series(dropna(x), Mean()), x->reduce(merge, x), xs).stats[1].μ

Base.std(xs::DArray{<:DataValue}) =
  √Dagger.reduceblock(x->Series(dropna(x), Variance()), x->reduce(merge, x), xs).stats[1].σ2
