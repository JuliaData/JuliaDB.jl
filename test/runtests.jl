using JuliaDB
using Base.Test
using TextParse
using IndexedTables
using NamedTuples

@testset "Utilities" begin

    @testset "NamedTuples isless" begin
        @test @NT(x=>1, y=>2) <  @NT(x=>1, y=>2.5)
        @test @NT(x=>1, y=>2) >= @NT(x=>1, y=>2)
        @test @NT(x=>1, y=>2) <  @NT(x=>1, y=>2, z=>3)
    end

    @testset "NamedTuples map" begin
        @test map(round,
                  @NT(x=>1//3, y=>Int),
                  @NT(x=>3, y=>2//3)) == @NT(x=>0.333, y=>1)
    end
end

import JuliaDB: Interval, hasoverlap

@testset "Interval" begin

    @testset "hasoverlap" begin
        @test hasoverlap(Interval(0,2), Interval(1,2))
        @test !(hasoverlap(Interval(2,1), Interval(1,2)))
        @test hasoverlap(Interval(0,2), Interval(0,2))
        @test hasoverlap(Interval(0,2), Interval(-1,2))
        @test hasoverlap(Interval(0,2), Interval(2,3))
        @test !hasoverlap(Interval(0,2), Interval(4,5))
    end

    @testset "in" begin
        @test 1 in Interval(0, 2)
        @test !(1 in Interval(2, 1))
        @test !(3 in Interval(0, 2))
        @test !(-1 in Interval(0, 2))
    end

end

@testset "Load" begin
    path = joinpath(dirname(@__FILE__), "..","test","fxsample", "*.csv")
    files = glob(path[2:end], "/")
    dt = load(files, header_exists=false, type_detect_rows=4)
    allcsv = reduce(string, readstring.(files))
    nds = loadNDSparse(allcsv;
                 csvread=TextParse._csvread,
                 type_detect_rows=4,
                 header_exists=false)
    @test gather(dt) == nds
end

@testset "Getindex" begin
    nds = NDSparse(Columns(rand(["X","Y"], 100), rand(1:20, 100)), rand(100), agg=+)
    dt = distribute(nds, 10)
    @test gather(dt[["X", "Y"], 2:8]) == nds[["X", "Y"], 2:8]
end

