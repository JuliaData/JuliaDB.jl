const TextMIME = Union{MIME"text/plain", MIME"text/html"}
function take_n(t::DTable, n)
    required = n
    getter(required, c) = collect(delayed(x->subtable(x, 1:min(required, length(x))))(c))

    i = 1
    top = getter(required, t.chunks[i])
    required = n - length(top)
    while required > 0 && 1 <= i < length(t.chunks)
        i += 1
        required = n - length(top)
        top = _merge(top, getter(required, t.chunks[i]))
    end
    return top
end

_indexnames(c::Dagger.Chunk) = [c.chunktype.types[3].types[1].name.names...]
_datanames( c::Dagger.Chunk) = [c.chunktype.types[4].types[1].name.names...]
_indexnames(t::DTable) = _indexnames(first(t.chunks))
_datanames( t::DTable) = _datanames( first(t.chunks))

_indextypes(c::Dagger.Chunk) = map(eltype, c.chunktype.types[3].types[1].types)
_datatypes( c::Dagger.Chunk) = map(eltype, c.chunktype.types[4].types[1].types)
_indextypes(t::DTable) = _indextypes(first(t.chunks))
_datatypes( t::DTable) = _datatypes( first(t.chunks))

function Base.show(io::IO, t::DTable)
    # we fetch at most 21 elements and let IndexedTable
    # display it.
    len = trylength(t)
    if !isempty(t.chunks)
        # top = take_n(t, 5)
        nchunks = length(t.chunks)
        print(io, "DTable with ")
        if !isnull(len)
            print(io, "$(get(len)) rows in ")
        end

        println(io, "$nchunks chunks.\n")

        # Index
        index_names = _indexnames(t)
        index_types = _indextypes.(t)
        data_names  = _datanames(t)
        data_types  = _datatypes.(t)

        lngth = max(maximum(length∘string, index_names), maximum(length∘string, data_names)) + 5

        # Index
        println(io, rpad("Index names:", lngth), "Index type:")
        println.(io, rpad.(index_names, lngth) .* string.(index_types))
        println()

        # Data
        println(io, rpad("Data names:", lngth), "Data type:")
        println.(io, rpad.(data_names, lngth) .* string.(data_types))

        # println(io, "")
        # show(io, top)
        # if isnull(len) || get(len) > 5
        #     println(io, "")
        #     print(io, "...")
        # end
    else
        println(io, "an empty DTable")
    end
end
