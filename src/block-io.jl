##
# give a stream view to a block from any seekable iostream
# By @tanmaykm
#
import Base: close, eof, read, read!, peek, seek, write, filesize, position, seekend, seekstart, skip, bytesavailable
using DelimitedFiles

###############################################################################
#  BlockIO
###############################################################################

struct BlockIO <: IO
    s::IO
    r::UnitRange
    l::Int
end

function find_end_pos(bio::BlockIO, end_byte::Char)
    seekend(bio)
    try
        while(!eof(bio.s) && (end_byte != read(bio, Char))) continue end
    catch
    end
    position(bio.s)
end

function find_start_pos(bio::BlockIO, end_byte::Char)
    (bio.r.start == 1) && (return bio.r.start)
    seekstart(bio)
    !eof(bio.s) && while(end_byte != read(bio, Char)) continue end
    position(bio.s)+1
end

function BlockIO(s::IO, r::UnitRange, match_ends::Union{Char,Nothing}=nothing)
    # TODO: use mark when available
    seekend(s)
    ep = position(s)

    r = min(r.start,ep+1):min(r.start+length(r)-1,ep)
    bio = BlockIO(s, r, length(r))
    if match_ends !== nothing
        p1 = find_start_pos(bio, match_ends)
        p2 = find_end_pos(bio, match_ends)
        r = p1:p2
        bio = BlockIO(s, r, length(r))
    end
    seekstart(bio)
    bio
end

BlockIO(bio::BlockIO, match_ends::Union{Char,Nothing}=nothing) = BlockIO(bio.s, bio.r, match_ends)

close(bio::BlockIO) = close(bio.s)
eof(bio::BlockIO) = (position(bio) >= bio.l)
read(bio::BlockIO, x::Type{UInt8}) = read(bio.s, x)
read!(bio::BlockIO, a::Vector{UInt8}) = (length(a) <= bytesavailable(bio)) ? read!(bio.s, a) : throw(EOFError())
read!(bio::BlockIO, a::Array{T}) where {T} = (length(a)*sizeof(T) <= bytesavailable(bio)) ? read!(bio.s, a) : throw(EOFError())

read(bio::BlockIO, nb::Integer = bio.l) = String(read!(bio, Array{UInt8}(undef, nb)))

peek(bio::BlockIO) = peek(bio.s)
write(bio::BlockIO, p::Ptr, nb::Integer) = write(bio, p, int(nb))
write(bio::BlockIO, p::Ptr, nb::Int) = write(bio.s, p, nb)
write(bio::BlockIO, x::UInt8) = write(bio, UInt8[x])
write(bio::BlockIO, a::Array{T}, len) where {T} = write_sub(bio, a, 1, len)
write(bio::BlockIO, a::Array{T}) where {T} = write(bio, a, length(a))
write_sub(bio::BlockIO, a::Array{T}, offs, len) where {T} = isbits(T) ? write(bio, pointer(a,offs), len*sizeof(T)) : error("$T is not bits type")

bytesavailable(bio::BlockIO) = (bio.l - position(bio))
position(bio::BlockIO) = position(bio.s) - bio.r.start + 1

filesize(bio::BlockIO) = bio.l

seek(bio::BlockIO, n::Integer) = seek(bio.s, n+bio.r.start-1)
seekend(bio::BlockIO) = seek(bio, filesize(bio))
seekstart(bio::BlockIO) = seek(bio, 0)
skip(bio::BlockIO, n::Integer) = seek(bio, n+position(bio))

# it returns a IOBuffer, not String
function readuntil(bio::BlockIO, dlm::Char, n::Int)
    io = IOBuffer()
    i = 0
    while !eof(bio)
        c = read(bio, Char)
        write(io, c)
        c == dlm && (i += 1)
        i == n && break
    end
    seekstart(io)
end

blocksize(s, n) = ceil(Int, s / n)

function blocks(fname, dlm = '\n', n = length(workers()))
    s = filesize(fname)
    bs = blocksize(s, n)
    @assert(bs > 1, "Specified more blocks than bytes in file")

    start = 1
    stop = 1
    offset = 0  # bs offset
    bios = Vector{BlockIO}(undef, n)

    for i ∈ 1:n
        stop = start + offset + bs - 1
        r = start:stop
        bio = BlockIO(open(fname), r, dlm)
        stop′ = bio.r.stop  # get relocated ending position
        start = stop′
        offset = stop′ - stop
        bios[i] = bio
    end

    @assert sum(filesize.(bios)) == s

    bios
end

###############################################################################
#  ChunkIter
###############################################################################

struct ChunkIter
    io::IO
    dlm::Char
    n::Int
end

ChunkIter(fname::String, dlm::Char = '\n', n::Int = 1000) =
    ChunkIter(open(fname), dlm, n)

function Base.iterate(i::ChunkIter, idx=1)
    eof(i.io) && return nothing
    return readuntil(i.io, i.dlm, i.n), idx+1
end

###############################################################################
#  Usage
###############################################################################

# addprocs()
# fname = "path/to/file.csv"
# bios = JuliaDB.blocks(fname)
# b = bios[1]
# iter = ChunkIter(b, '\n', 10)
# readdlm(next(iter, 42)[1], ',')
