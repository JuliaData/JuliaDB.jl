const TextMIME = Union{MIME"text/plain", MIME"text/html"}
function take_n(t::DTable, n)
    chunkcol = chunks(t).data.columns.chunk

    required = n
    getter(required, c) = gather(delayed(x->subtable(x, 1:min(required, length(x))))(c))

    i = 1
    top = getter(required, chunkcol[i])
    required = n - length(top)
    while required > 0 && 1 <= i < length(chunkcol)
        i += 1
        required = n - length(top)
        top = _merge(top, getter(required, chunkcol[i]))
    end
    return top
end

function Base.show(io::IO, t::DTable)
    # we fetch at most 21 elements and let IndexedTable
    # display it.
    len = trylength(t)
    if !isempty(t.chunks)
        top = take_n(t, 5)
        nchunks = length(chunks(t))
        print(io, "DTable with ")
        if !isnull(len)
            print(io, "$(get(len)) rows in ")
        end

        println(io, "$nchunks chunks:")
        println(io, "")
        show(io, top)
        println(io, "")
        print(io, "...")
    else
        println(io, "an empty DTable")
    end
end
