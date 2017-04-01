using JuliaDB
using Base.Test
using TextParse
using IndexedTables
using NamedTuples
using PooledArrays

import JuliaDB: MmappableArray, copy_mmap, unwrap_mmap, tuplesetindex
@testset "Utilities" begin

    @testset "NamedTuples isless" begin
        @test @NT(x=1, y=2) <  @NT(x=1, y=2.5)
        @test @NT(x=1, y=2) >= @NT(x=1, y=2)
        @test @NT(x=1, y=2) <  @NT(x=1, y=2, z=3)
    end

    @testset "NamedTuples map" begin
        @test map(round,
                  @NT(x=1//3, y=Int),
                  @NT(x=3, y=2//3)) == @NT(x=0.333, y=1)
    end
    @testset "tuplesetindex" begin
        @test tuplesetindex((1,2,3), 4, 2) == (1,4,3)
        @test tuplesetindex(@NT(x=1,y=2,z=3), 4, 2) == @NT(x=1,y=4,z=3)
        @test tuplesetindex(@NT(x=1,y=2,z=3), 4, :y) == @NT(x=1,y=4,z=3)
    end
    @testset "MmappableArray" begin
        @testset "Array of floats" begin
            X = rand(100, 100)
            f = tempname()
            M = MmappableArray(f, X)
            sf = tempname()
            open(io -> serialize(io, M), sf, "w")
            @test filesize(sf) < 100 * 100
            M2 = open(deserialize, sf)
            @test X == M
            @test M == M2
        end

        @testset "PooledArray" begin
            P = PooledArray(rand(["A", "B"], 10^3))
            f = tempname()
            P1 = MmappableArray(f, P)
            psf = tempname()
            open(io -> serialize(io, P1), psf, "w")
            @test filesize(psf) < 10^3
            P2 = open(deserialize, psf)
            @test P2 == P1
        end

        @testset "DataTime Array" begin
            t = Int(Dates.value(now()))
            T = DateTime.(map(x->round(Int,x), linspace(t-10^7, t, 10^3) |> collect))
            f = tempname()
            M = MmappableArray(f, T)
            sf = tempname()
            open(io -> serialize(io, M), sf, "w")
            @test filesize(sf) < 1000
            @test open(deserialize, sf) == T
        end

        @testset "IndexedTable" begin
            P = PooledArray(rand(["A", "B"], 10^4))
            t = Int(Dates.value(now()))
            T = DateTime.(map(x->round(Int,x), linspace(t-10^7, t, 10^4) |> collect))
            nd = IndexedTable(Columns(P, T), Columns(rand(10^4), rand(10^4)), copy=false, presorted=true)
            ndf = tempname()
            mm = copy_mmap(ndf, nd)
            ndsf = tempname()
            open(io -> serialize(io, mm), ndsf, "w")
            @test filesize(ndsf) < 10^4
            @test open(deserialize, ndsf) == nd
            nd2 = open(deserialize, ndsf)
            @test nd == unwrap_mmap(nd2)
            @test typeof(nd) == typeof(unwrap_mmap(nd2))
        end
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
const fxdata_dist = loadfiles(files, header_exists=false, type_detect_rows=4, indexcols=1:2, usecache=false)
allcsv = reduce(string, readstring.(files))
const fxdata, _ = loadTable(allcsv;
                            csvread=TextParse._csvread,
                            indexcols=1:2,
                            type_detect_rows=4,
                            header_exists=false)

ingest_output = tempname()
fxdata_ingest = ingest(files, ingest_output, header_exists=false, type_detect_rows=4, indexcols=1:2)

@testset "extractarray" begin
    @test gather(getindexcol(fxdata_ingest, 1)) == fxdata.index.columns[1]
    @test gather(getindexcol(fxdata_ingest, 2)) == fxdata.index.columns[2]
    @test gather(getdatacol(fxdata_ingest, 1)) == fxdata.data.columns[1]
    @test gather(getdatacol(fxdata_ingest, 2)) == fxdata.data.columns[2]
end

@testset  "printing" begin
    x = distribute(IndexedTable([1], [1]), 1)
    @test sprint(io -> show(io, x)) == """
    DTable with 1 rows in 1 chunks:

    ──┬──
    1 │ 1"""
end


@testset "Load" begin
    cache = joinpath(JuliaDB.JULIADB_CACHEDIR, JuliaDB.JULIADB_FILECACHE)
    if isfile(cache)
        rm(cache)
    end
    @test gather(fxdata_dist) == fxdata
    @test gather(fxdata_dist) == fxdata
    @test gather(fxdata_ingest) == fxdata
    @test gather(load(ingest_output)) == fxdata
    #@test gather(dt[["blah"], :,:]) == fxdata
    function common_test1(dt)
        nds=gather(dt)
        @test haskey(nds.index.columns, :symbol)
        @test haskey(nds.index.columns, :time)
        @test length(nds.index.columns) == 2
        @test haskey(nds.data.columns, :open)
        @test haskey(nds.data.columns, :close)
        @test length(nds.data.columns) == 2
    end
    dt = loadfiles(files, colnames=["symbol", "time", "open", "close"], indexcols=["symbol", "time"], usecache=false)
    common_test1(dt)
    dt = loadfiles(files, colnames=["symbol", "time", "open", "close"], datacols=["open", "close"], usecache=false)
    common_test1(dt)
    dt = loadfiles(files, colnames=["symbol", "time", "open", "close"], datacols=["open", "close"], indexcols=["symbol", "time"], usecache=false)
    common_test1(dt)
    dt = loadfiles(files, colnames=["symbol", "time", "open", "close"], usecache=false)
    nds = gather(dt)
    @test length(nds.data.columns) == 1
    @test !isempty(nds.data.columns.close)
    @test length(nds.index.columns) == 3
end

@testset "Getindex" begin
    idx = Columns(rand(["X","Y","Z"], 1000),
                  vcat(rand(1:12, 250), rand(10:20, 250),
                       rand(15:30, 250), rand(23:43, 250)))

    nds = IndexedTable(idx, rand(1000), agg=+)

    dt = distribute(nds, 43)
    @test gather(dt[:, 2:8]) == nds[:, 2:8]

    I = rand(1:length(nds), 43)
    for i in I
        idx = nds.index[i]
        @test dt[idx...] == nds[idx...]
    end

    @test gather(fxdata_dist[["AUD/USD", "CAD/JPY"], :]) == fxdata[["AUD/USD", "CAD/JPY"], :]
    @test gather(load(ingest_output)[["AUD/USD", "CAD/JPY"], :]) == fxdata[["AUD/USD", "CAD/JPY"], :]
    x = save(load(ingest_output)[["AUD/USD", "CAD/JPY"], :], tempname())
    @test gather(x) == fxdata[["AUD/USD", "CAD/JPY"], :]
end

@testset "Select" begin
    involving(sym, x) = startswith(x, sym) || endswith(x, sym)
    query = (1=>x-> involving("USD", x) || involving("AUD", x), 2=>x->Base.Dates.month(x)==3)
    @test select(fxdata, query...) == gather(select(fxdata_dist, query...))
end

@testset "Convertdim" begin
    _plus(x,y) = map(+,x, y)
    step1 = convertdim(fxdata, 1, x->x[1:3]; agg=_plus)
    @test step1 ==
        gather(convertdim(fxdata_dist, 1, x->x[1:3]; agg=_plus))

    dt = load(ingest_output)
    chain = convertdim(convertdim(dt, 1, x->x[1:3]; agg=_plus), 2, Date, agg=_plus)
    @test convertdim(step1, 2, Date; agg=_plus) == gather(chain)
    @test convertdim(step1, 2, Date; agg=_plus) == gather(save(chain, tempname()))
    dt2 = save(convertdim(dt, 1, x->x[1:3]; agg=_plus), tempname())
    chainvec = convertdim(dt2, 2, Date, vecagg=length)
    step2 = convertdim(step1, 2, Date; vecagg=length)
    @test step2 == gather(chainvec)
    @test gather(chainvec[["AUD", "USD"], :]) == step2[["AUD", "USD"], :]
end

@testset "permutedims" begin
    t = IndexedTable(Columns([1,1,2,2], ["a","b","a","b"]), [1,2,3,4])
    for n=1:5
        d = distribute(t, n)
        @test gather(permutedims(d, [2,1])) == permutedims(t, [2,1])
    end
end

include("join.jl")
