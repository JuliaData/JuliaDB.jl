using Base.Test
using JuliaDB
using PooledArrays

import JuliaDB: MmappableArray, copy_mmap, unwrap_mmap

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

import Dagger: Chunk, MemToken
import JuliaDB: OnDisk
@testset "Load" begin
    cache = joinpath(JuliaDB.JULIADB_CACHEDIR, JuliaDB.JULIADB_FILECACHE)
    if isfile(cache)
        rm(cache)
    end
    @test gather(fxdata_dist) == fxdata
    @test gather(fxdata_dist) == fxdata
    @test gather(fxdata_ingest) == fxdata
    @test gather(load(ingest_output)) == fxdata
    c = first(load(ingest_output).chunks.data.columns.chunk)
    @test typeof(c.handle) == OnDisk
    d = load(ingest_output,tomemory=true)
    @test gather(d) == fxdata
    c2 = first(d.chunks.data.columns.chunk)
    @test typeof(c2.handle) == MemToken
    #@test gather(dt[["blah"], :,:]) == fxdata
    function common_test1(dt)
    end
    dt = loadfiles(files, colnames=["symbol", "time", "open", "close"], indexcols=["symbol", "time"], usecache=false)
    common_test1(dt)
    nds=gather(dt)
    @test haskey(nds.index.columns, :symbol)
    @test haskey(nds.index.columns, :time)
    @test length(nds.index.columns) == 2
    @test haskey(nds.data.columns, :open)
    @test haskey(nds.data.columns, :close)
    @test length(nds.data.columns) == 2
    dt = loadfiles(files, colnames=["symbol", "time", "open", "close"], usecache=false)
    dt = loadfiles(files, colnames=["symbol", "time", "open", "close"], usecache=false)
    nds = gather(dt)
    @test length(nds.data.columns) == 1
    @test !isempty(nds.data.columns.close)
    @test length(nds.index.columns) == 3
end
