import JuliaDB.ML
using JuliaDB
using Test
using OnlineStats
using PooledArrays
using DataValues

@testset "feature extraction" begin
    @testset "schema" begin
        @test ML.schema([1:10;]).series == fit!(Series(Variance()), [1:10;])
        x = repeat([1,2], inner=5)
        @test ML.schema(x, ML.Categorical).series == fit!(Series(CountMap(Int)), x) == ML.schema(PooledArray(x)).series
        m = ML.schema(DataValueArray(x))
        @test m isa ML.Maybe
        @test m.feature.series == ML.schema(x).series
        t = table(PooledArray([1,2,1,2]), [1,2,3,4], names=[:x, :y])
        sch = ML.schema(t)
        @test sch[:x].series == ML.schema(PooledArray([1,2,1,2])).series
        @test sch[:y].series == ML.schema([1,2,3,4]).series
    end

    @testset "featuremat" begin
        @test ML.featuremat([1,3,5]) == [-1.0, 0, 1.0]'
        Δ = 0.5 / std([1,2])
        @test ML.featuremat(DataValueArray([1,2,3], Bool[0,0,1])) ≈ [0 0 1; -Δ Δ 0]
        @test isempty(ML.featuremat(["x","y","x"]))
        @test ML.featuremat(PooledArray(["x","y","x"])) == [1  0  1; 0  1  0]

        t = table(PooledArray([1,1,2,2]), [1,2,3,4], DataValueArray([1,2,3,4]), ["x", "y", "z", "a"], names=[:a,:b,:c,:d])
        @test ML.featuremat(t) == collect(ML.featuremat(distribute(t, 2)))

        x = randn(100)
        @test ML.featuremat(x) ≈ ((x .- mean(x)) ./ std(x))'
    end

    @testset "Issue 149" begin
        x = rand(10), rand(1:4, 10), rand(1:4, 10)
        t  = table(x..., names = [:x1, :x2, :x3])
        td = table(x..., names = [:x1, :x2, :x3], chunks = 2)
        sch = ML.schema(t, hints=Dict(
            :x1 => ML.Continuous,
            :x2 => ML.Categorical,
            :x3 => ML.Categorical,
            )
        )
        schd = ML.schema(td, hints=Dict(
            :x1 => ML.Continuous,
            :x2 => ML.Categorical,
            :x3 => ML.Categorical,
            )
        )

        @test schd[:x1].series.stats[1].σ2 ≈ sch[:x1].series.stats[1].σ2
        @test schd[:x2].series == sch[:x2].series
        @test schd[:x3].series == sch[:x3].series
    end
end
