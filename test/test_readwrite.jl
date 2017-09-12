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
        rm(sf)
        rm(f)
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
        rm(psf)
        rm(f)
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
        rm(sf)
        rm(f)
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
        rm(ndsf)
        rm(ndf)
    end
end

path = joinpath(dirname(@__FILE__), "..","test", "sample")
files = glob("*.csv", path)
const spdata_dist = loadfiles(files, type_detect_rows=4,
                              indexcols=1:2, usecache=false)
const spdata_dist_path = loadfiles(path, type_detect_rows=4,
                              indexcols=1:2, usecache=false)

_readstr(f) = open(f) do fh
    readline(fh)
    readstring(fh)
end
readfiles(fs) = reduce(string, vcat(readstring(fs[1]), _readstr.(fs[2:end])))
allcsv = reduce(string, readfiles(files))
const spdata = load_table(allcsv;
                            csvread=TextParse._csvread,
                            header_exists=true,
                            indexcols=1:2)

shuffle_files = shuffle(files)
shuffle_allcsv = reduce(string, readfiles(shuffle_files))
const spdata_unordered = load_table(shuffle_allcsv;
                                      csvread=TextParse._csvread,
                                      indexcols=[])

ingest_output = tempname()
spdata_ingest = ingest(files, ingest_output, indexcols=1:2)
ingest_output_unordered = tempname()
# note: this will result in a different table if files[3:end] is ingested first
spdata_ingest_unordered = ingest(shuffle_files[1:3], ingest_output_unordered,
                                 indexcols=[])
# this should also test appending new files
spdata_ingest_unordered = ingest!(shuffle_files, ingest_output_unordered,
                                 indexcols=[])

import Dagger: Chunk, MemToken
import JuliaDB: OnDisk
@testset "Load" begin
    cache = joinpath(JuliaDB.JULIADB_DIR, JuliaDB.JULIADB_FILECACHE)
    if isfile(cache)
        rm(cache)
    end
    missingcoltbl = loadfiles(joinpath(@__DIR__, "missingcols"), datacols=[:a, :x, :y], usecache=false)
    @test eltype(missingcoltbl) == @NT(a::Int, x::Nullable{Int}, y::Nullable{Float64})
    # file name as a column:
    @test unique(keys(loadfiles(path, indexcols=[:year, :date, :ticker],filenamecol=:year, usecache=false), :year)|> collect) == string.(2010:2015)
    @test_throws ErrorException load_table("a,b\n,2", csvread=TextParse._csvread, indexcols=[1])
    @test collect(spdata_dist) == spdata
    @test collect(spdata_dist_path) == spdata
    @test collect(spdata_ingest) == spdata
    @test collect(load(ingest_output)) == spdata
    @test collect(load(ingest_output_unordered)) == spdata_unordered
    @test issorted(collect(keys(load(ingest_output_unordered), 1)))
    c = first(load(ingest_output).chunks)
    @test typeof(c.handle) == OnDisk
    @test c.handle.cached_on == []
    d = load(ingest_output,tomemory=true)
    @test collect(d) == spdata
    c2 = first(d.chunks)
    @test typeof(c2.handle) == MemToken
    #@test collect(dt[["blah"], :,:]) == spdata
    dt = loadfiles(files, indexcols=[("date", "dummy"), ("dummy", "ticker")], usecache=false)
    nds=collect(dt)
    @test haskey(nds.index.columns, :date)
    @test haskey(nds.index.columns, :dummy)
    @test !haskey(nds.index.columns, :ticker)
    @test length(nds.index.columns) == 2
    @test fieldnames(nds.data.columns) == [:open, :high, :low, :close, :volume]
    @test length(nds.data.columns) == 5

    dt = loadfiles(shuffle_files, usecache=false)
    @test collect(dt) == spdata_unordered
    @test issorted(collect(keys(dt, 1)))
    # reuses csv read cache:
    dt = loadfiles(shuffle_files, indexcols=[], chunks=4, usecache=false)
    @test collect(dt) == spdata_unordered
    dt = loadfiles(shuffle_files, indexcols=[], chunks=4) # cache test
    @test collect(dt) == spdata_unordered

    # test specifying column names
    dt = loadfiles(files[1:2], indexcols=[:a,:b], colnames=[:a,:b,:c,:d,:e,:f,:g], usecache=false, header_exists=false)
    nds = collect(dt)
    @test haskey(nds.index.columns, :a)
    @test haskey(nds.index.columns, :b)
    @test fieldnames(nds.data.columns) == [:c,:d,:e,:f,:g]
end

@testset "save" begin
    t = IndexedTable([1,2,3,4], [1,2,3,4])
    n = tempname()
    x = JuliaDB.save(distribute(t, 4), n)
    @test !any(c->isempty(c.handle.cached_on), x.chunks)
    t1 = load(n)
    @test all(c->isempty(c.handle.cached_on), t1.chunks)
    rm(n, recursive=true)
end

rm(ingest_output, recursive=true)
rm(ingest_output_unordered, recursive=true)
