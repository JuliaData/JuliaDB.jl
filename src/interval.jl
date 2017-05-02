using Base.Test

"""
An interval type tailored specifically to store intervals of
indices of an Table object. Some of the operations on this
like `in` or `<` may be controversial for a generic Interval type.
"""
immutable Interval{T}
    first::T
    last::T
end

# desired properties:
Base.eltype{T}(int::Interval{T}) = T
Base.first(int::Interval) = int.first
Base.last(int::Interval) = int.last
Base.isempty(int::Interval) = first(int) > last(int)
Base.isless(x::Interval, y::Interval) = x.last < y.first
Base.in(x, int::Interval) = first(int) <= x <= last(int)
Base.in(x::Range, int::Interval) = hasoverlap(Interval(first(x),last(x)), int)
Base.in(x::AbstractArray, int::Interval) = any(a in int for a in x)
Base.in(x::Colon, int::Interval) = true
Base.in(int::Interval, x::Union{AbstractArray, Colon}) = x in int

function hasoverlap(i1::Interval, i2::Interval)
    (isempty(i1) || isempty(i2)) && return false

    (first(i2) <= last(i1) && first(i1) <= last(i2)) ||
    (first(i1) <= last(i2) && first(i2) <= last(i1))
end

function boxhasoverlap(a,b)
    all(map(in, map(Interval, first(a), last(a)),
                map(Interval, first(b), last(b))))
end

# KIND OF A HACK: Interval of Intervals - used for indexing into a table of Intervals

# convert a thing to an interval of its own
Interval(x) = Interval(x,x)

# An interval of intervals can be used to do binary search on
# a sorted list of intervals. If you just give an interval, Table is going
# to think you are doing scalar indexing.
IntervalInterval(x, y) = Interval(Interval(x), Interval(y))
Base.in{T}(x::Interval{T}, y::Interval{T}) = hasoverlap(x,y)
Base.in{T}(x::Interval{T}, y::Interval{Interval{T}}) = x in Interval(first(first(y)),last(last(y)))

function Base.intersect(i1::Interval, i2::Interval)
    Interval(max(first(i1), first(i2)), min(last(i1), last(i2)))
end
function Base.merge(i1::Interval, i2::Interval)
    Interval(min(first(i1), first(i2)), max(last(i1), last(i2)))
end

function _map(f, i::Interval)
    fst, lst = f(first(i)), f(last(i))
    if lst < fst
        throw(ArgumentError(
            "map on $(typeof(i)) is only allowed on monotonically increasing functions"
           )
        )
    end
    Interval(fst, lst)
end

Base.show(io::IO, i::Interval) = (show(io, i.first); print(io, ".."); show(io, i.last))
