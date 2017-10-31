using JuliaDB
using Base.Test

@testset "rechunk" begin
    t = IndexedTable([1:10;], ones(10), ones(10))
    d = distribute(t, 2)
    d2 = compute(rechunk(d, [2,3,4,1]))
    @test get.(map(JuliaDB.nrows, d2.subdomains)) == [2,3,4,1]
end

@testset "rechunk by" begin
    t = distribute(IndexedTable(Columns([1,1,1,2,2,2], [1,1,2,2,3,3]), [1:6;]), [2,2,2])
    t1=rechunk(t, by=(1, as(x->x.>=2,2, 2)), closed=true)
    @test collect(t1) == collect(t)
    @test collect(t1.chunks[1]) == IndexedTable(Columns([1,1],[1,1]), [1,2])
    @test collect(t1.chunks[2]) == IndexedTable(Columns([1],[2]), [3])
    @test collect(t1.chunks[3]) == IndexedTable(Columns([2,2,2],[2,3,3]), [4,5,6])
end
