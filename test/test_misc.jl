using JuliaDB
using Base.Test

@testset "extractarray" begin

    t = IndexedTable(Columns(a=[1,1,1,2,2], b=[1,2,3,1,2]),
                     Columns(c=[1,2,3,4,5], d=[5,4,3,2,1]))
    for i=[2, 3, 5]
        d = compute(distribute(t, i))
        dist = map(get, chunks(d).data.columns.length)
        @test map(length, Dagger.domainchunks(getindexcol(d, 1).result)) == dist
        @test gather(getindexcol(d, 2)) == t.index.columns[2]
        @test gather( getdatacol(d, 2)) == t.data.columns[2]
    end
end

@testset  "printing" begin
    x = distribute(IndexedTable([1], [1]), 1)
    @test sprint(io -> show(io, x)) == """
    DTable with 1 rows in 1 chunks:

    ──┬──
    1 │ 1"""
end

import JuliaDB: chunks, index_spaces, has_overlaps
@testset "has_overlaps" begin
    t = IndexedTable(Columns([1,1,2,2,2,3], [1,2,1,1,2,1]), [1,2,3,4,5,6])
    d = distribute(t, [2,3,1])
    i = index_spaces(chunks(d))
    @test !has_overlaps(i)
    @test !has_overlaps(i, true)

    d = distribute(t, [2,2,2])
    i = index_spaces(chunks(d))
    @test !has_overlaps(i)
    @test !has_overlaps(i, true)

    d = distribute(t, [2,1,3])
    i = index_spaces(chunks(d))
    @test !has_overlaps(i)
    @test has_overlaps(i, true)
end
