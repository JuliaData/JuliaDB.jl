using NamedTuples
using Compat

using Base.Test

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

let
    @test @NT(x=>1, y=>2) <  @NT(x=>1, y=>2.5)
    @test @NT(x=>1, y=>2) >= @NT(x=>1, y=>2)
    @test @NT(x=>1, y=>2) <  @NT(x=>1, y=>2, z=>3)
end

@generated function Base.map(f, nts::NamedTuple...)
    fields = fieldnames(nts[1])
    if !all(map(x->isequal(fieldnames(x), fields), nts[2:end]))
        throw(ArgumentError("All NamedTuple inputs to map must have the same fields"))
    end
    N = length(fields)
    args = ntuple(N) do i
        :($(fields[i]) => f(map(t->t[$i], nts)...))
    end
    :(@NT($(args...)))
end

let
    @test map(round, @NT(x=>1//3, y=>Int), @NT(x=>3, y=>2//3)) == @NT(x=>0.333, y=>1)
end

immutable Interval{T}
    first::T
    last::T
end
Base.eltype{T}(int::Interval{T}) = T
Base.first(int::Interval) = int.first
Base.last(int::Interval) = int.last
Base.in(x, int::Interval) = first(int) <= x <= last(int)
Base.in(x::Range, int::Interval) = first(x) in int || last(x) in int
Base.in(x::AbstractArray, int::Interval) = any(a in int for a in x)
Base.in(x::Colon, int::Interval) = any(a in int for a in x)
Base.isempty(int::Interval) = first(int) > last(int)
Base.isless(x::Interval, y::Interval) = x.last < y.first

function hasoverlap(i1::Interval, i2::Interval)
    (isempty(i1) || isempty(i2)) && return false

    (first(i2) <= last(i1) && first(i1) <= last(i2)) ||
    (first(i1) <= last(i2) && first(i2) <= last(i1))
end

# KIND OF A HACK: Interval of Intervals - used for indexing into a table of Intervals

# convert a thing to an interval of its own
Interval(x) = Interval(x,x)

# An interval of intervals can be used to do binary search on
# a sorted list of intervals. If you just give an interval, NDSparse is going
# to think you are doing scalar indexing.
IntervalInterval(x, y) = Interval(Interval(x), Interval(y))
Base.in{T}(x::Interval{T}, y::Interval{T}) = hasoverlap(x,y)
Base.in{T}(x::Interval{T}, y::Interval{Interval{T}}) = x in Interval(first(first(y)),last(last(y)))

function Base.intersect(i1::Interval, i2::Interval)
    Interval(max(first(i1), first(i2)), min(last(i1), last(i2)))
end

let
    @test hasoverlap(Interval(0,2), Interval(1,2))
    @test !(hasoverlap(Interval(2,1), Interval(1,2)))
    @test hasoverlap(Interval(0,2), Interval(0,2))
    @test hasoverlap(Interval(0,2), Interval(-1,2))
    @test hasoverlap(Interval(0,2), Interval(2,3))
    @test !hasoverlap(Interval(0,2), Interval(4,5))
end

let
    @test 1 in Interval(0, 2)
    @test !(1 in Interval(2, 1))
    @test !(3 in Interval(0, 2))
    @test !(-1 in Interval(0, 2))
end
