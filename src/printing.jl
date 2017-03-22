const TextMIME = Union{MIME"text/plain", MIME"text/html"}
function take_n(t::DTable, n)
    chunkcol = chunks(t).data.columns.chunk

    i = 1
    getter(c) = gather(delayed(x->subtable(x, 1:min(i, length(x))))(c))
    top = getter(chunkcol[i])
    while length(top) < n && 1 <= i <= length(chunkcol)
        i += 1
        top = _merge(top, getter(chunkcol[i]))
    end
    return top
end

function Base.show(io::IO, t::DTable)
    # we fetch at most 21 elements and let NDSparse
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
        println(io, "...")
    else
        println(io, "an empty DTable")
    end
end
