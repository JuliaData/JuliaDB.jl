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

path = joinpath(dirname(@__FILE__), "..","test","fxsample", "*.csv")
files = glob(path[2:end], "/")
const fxdata_dist = load(files, header_exists=false, type_detect_rows=4, indexcols=1:2)
allcsv = reduce(string, readstring.(files))
const fxdata = loadNDSparse(allcsv;
             csvread=TextParse._csvread,
             indexcols=1:2,
             type_detect_rows=4,
             header_exists=false)

@testset "Load" begin
    @test gather(fxdata_dist) == fxdata
    @test gather(fxdata_dist) == fxdata
    #@test gather(dt[["blah"], :,:]) == fxdata
    function common_test1(dt)
        nds=gather(dt)
        @test !isempty(nds.index.columns.symbol)
        @test !isempty(nds.index.columns.time)
        @test length(nds.index.columns) == 2
        @test !isempty(nds.data.columns.open)
        @test !isempty(nds.data.columns.close)
        @test length(nds.data.columns) == 2
    end
    dt = load(files, colnames=["symbol", "time", "open", "close"], indexcols=["symbol", "time"])
    common_test1(dt)
    dt = load(files, colnames=["symbol", "time", "open", "close"], datacols=["open", "close"])
    common_test1(dt)
    dt = load(files, colnames=["symbol", "time", "open", "close"], datacols=["open", "close"], indexcols=["symbol", "time"])
    common_test1(dt)
    dt = load(files, colnames=["symbol", "time", "open", "close"])
    nds = gather(dt)
    @test length(nds.data.columns) == 1
    @test !isempty(nds.data.columns.close)
    @test length(nds.index.columns) == 3
end

@testset "Getindex" begin
    idx = Columns(rand(["X","Y","Z"], 1000),
                  vcat(rand(1:12, 250), rand(10:20, 250),
                       rand(15:30, 250), rand(23:43, 250)))

    nds = NDSparse(idx, rand(1000), agg=+)

    dt = distribute(nds, 43)
    @test gather(dt[:, 2:8]) == nds[:, 2:8]

    I = rand(1:length(nds), 43)
    for i in I
        idx = nds.index[i]
        @test dt[idx...] == nds[idx...]
    end

    @test gather(fxdata_dist[["AUD/USD", "CAD/JPY"], :]) == fxdata[["AUD/USD", "CAD/JPY"], :]
end

@testset "Select" begin
    query = (1=>x->startswith(x,"USD") || endswith(x, "USD"), 2=>x->Base.Dates.month(x)==3)
    @test select(fxdata, query...) == gather(select(fxdata_dist, query...))
end

@testset "Convertdim" begin
    _plus(x,y) = map(+,x, y)
    step1 = convertdim(fxdata, 1, x->x[1:3]; agg=_plus)
    @test step1 ==
        gather(convertdim(fxdata_dist, 1, x->x[1:3]; agg=_plus))

    chain = convertdim(convertdim(fxdata_dist, 1, x->x[1:3]; agg=_plus), 2, Date, agg=_plus)
    @test convertdim(step1, 2, Date; agg=_plus) == gather(chain)
    chainvec = convertdim(convertdim(fxdata_dist, 1, x->x[1:3]; agg=_plus), 2, Date, vecagg=length)
    @test convertdim(step1, 2, Date; vecagg=length) == gather(chainvec)
end

