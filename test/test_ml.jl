import JuliaDB.ML
using Base.Test
using OnlineStats
using PooledArrays
using DataValues

@testset "feature extraction" begin
    @testset "schema" begin
        @test ML.schema([1:10;]).series == Series([1:10;], Mean(), Variance())
        x = repeat([1,2], inner=5)
        @test ML.schema(x, ML.Categorical).series == Series(x, CountMap(Int)) == ML.schema(PooledArray(x)).series
        m = ML.schema(DataValueArray(x))
        @test m isa ML.Maybe
        @test m.feature.series == ML.schema(x).series
        t = table(PooledArray([1,2,1,2]), [1,2,3,4], names=[:x, :y])
        sch = ML.schema(t)
        @test sch[:x].series == ML.schema(PooledArray([1,2,1,2])).series
        @test sch[:y].series == ML.schema([1,2,3,4]).series
    end

    @testset "featuremat" begin
        @test ML.featuremat([1,2,3]) == [-1.5, 0, 1.5]'
        @test ML.featuremat(DataValueArray([1,2,3], Bool[0,0,1])) == [0 0 1; -2 2 0]
        @test ML.featuremat(DataValueArray([1,2,3], Bool[0,0,1])) == [0 0 1; -2 2 0]
        @test isempty(ML.featuremat(["x","y","x"]))
        @test ML.featuremat(PooledArray(["x","y","x"])) == [1  0  1; 0  1  0]

        t = table(PooledArray([1,1,2,2]), [1,2,3,4], DataValueArray([1,2,3,4]), ["x", "y", "z", "a"], names=[:a,:b,:c,:d])
        @test ML.featuremat(t) == collect(ML.featuremat(dist(t)))
    end
end
