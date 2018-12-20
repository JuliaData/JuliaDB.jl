@testset "setindex!" begin
    x = ndsparse(([1,2],[2,3]), [4,5], chunks=2)
    x[2,3] = 6
    @test x == ndsparse(([1,2], [2,3]), [4,6])
    x[2,4] = 7
    @test x == ndsparse(([1,2,2], [2,3,4]), [4,6,7])

    x = ndsparse((x=[1,2],y=[2,3]), [4,5], chunks=2)
    x[2,3] = 6
    @test x == ndsparse((x=[1,2], y=[2,3]), [4,6])
    x[2,4] = 7
    @test x == ndsparse((x=[1,2,2], y=[2,3,4]), [4,6,7])
end

@testset "extractarray" begin

    t = NDSparse(Columns(a=[1,1,1,2,2], b=[1,2,3,1,2]),
                     Columns(c=[1,2,3,4,5], d=[5,4,3,2,1]))
    for i=[2, 3, 5]
        d = compute(distribute(t, i))
        dist = map(get, map(JuliaDB.nrows, d.domains))
        @test map(length, Dagger.domainchunks(keys(d, 1))) == dist
        @test collect(keys(d, 2)) == columns(t.index)[2]
        @test collect( values(d, 2)) == columns(t.data)[2]
    end
end

@testset  "printing" begin
    x = distribute(NDSparse([1], [1]), 1)
    @test repr(x) == """
    1-d Distributed NDSparse with 1 values (Int64) in 1 chunks:
    1 │
    ──┼──
    1 │ 1"""
end

import JuliaDB: chunks, index_spaces, has_overlaps
@testset "has_overlaps" begin
    t = NDSparse(Columns(([1,1,2,2,2,3], [1,2,1,1,2,1])), [1,2,3,4,5,6])
    d = distribute(t, [2,3,1])
    i = d.domains
    @test !has_overlaps(i, closed=false)
    @test !has_overlaps(i)

    d = distribute(t, [2,2,2])
    i = d.domains
    @test !has_overlaps(i, closed=false)
    @test !has_overlaps(i)

    d = distribute(t, [2,1,3])
    i = d.domains
    @test !has_overlaps(i, closed=false)
    @test has_overlaps(i)
end

import JuliaDB: with_overlaps, delayed
@testset "with_overlaps" begin
    t = NDSparse([1,1,2,2,2,3], [1,2,3,4,5,6])
    d = distribute(t, [2,2,2])
    group_count = 0
    t1 = with_overlaps(d) do cs
        if length(cs) > 1
            group_count += 1
            @test length(cs) == 2
            @test collect(cs[1]) == NDSparse([2,2], [3,4])
            @test collect(cs[2]) == NDSparse([2,3], [5,6])
            return delayed((x,y) -> merge(x,y,agg=nothing))(cs...)
        else
            cs[1]
        end
    end
    @test collect(groupreduce(+, t1)) == groupreduce(+, t)
    @test group_count == 1
end

import JuliaDB: subtable
@testset "subtable" begin
    t = NDSparse([1,2,3,4], [5,6,7,8])
    d = distribute(t, 3)

    for i=1:4
        @test collect(subtable(d, 1:i)) == subtable(t, 1:i)
        if i>=2
            @test collect(subtable(d, 2:i)) == subtable(t, 2:i)
        end
    end
end

@testset "Iterators.partition" begin
    t = compute(distribute(NDSparse([1:7;],[8:14;]), [3,2,2]))
    parts = map(NDSparse, Iterators.partition([1:7;], 2) |> collect,
                              Iterators.partition([8:14;], 2) |> collect)
    @test [p for p in Iterators.partition(t, 2)] == parts
end
