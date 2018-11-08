using JuliaDB
using Test

@testset "rechunk" begin
    t = NDSparse([1:10;], ones(10), ones(10))
    d = distribute(t, 2)
    d2 = compute(distribute(d, [2,3,4,1]))
    @test get.(map(JuliaDB.nrows, d2.domains)) == [2,3,4,1]
end

@testset "rechunk by" begin
    t = distribute(NDSparse(Columns([1,1,1,2,2,2], [1,1,2,2,3,3]), [1:6;]), [2,2,2])
    t1=rechunk(t, (1, 2=>x->x.>=2), closed=true, chunks=3)
    @test collect(t1) == selectkeys(collect(t), (1,2=>x->x>=2))
    @test collect(t1.chunks[1]) == NDSparse(Columns([1,1],[false,false]), [1,2])
  # @test collect(t1.chunks[2]) == NDSparse(Columns([1],[2]), [3])
  # @test collect(t1.chunks[3]) == NDSparse(Columns([2,2,2],[2,3,3]), [4,5,6])
end
