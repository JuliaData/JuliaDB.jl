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

function Base.show(io::IO, t::DTable)
    # we fetch at most 21 elements and let NDSparse
    # display it.
    len = trylength(t)
    if !isempty(t.chunks)
        top = take_n(t, 5)
        nchunks = length(t.chunks)
        print(io, "DTable with ")
        if !isnull(len)
            print(io, "$(get(len)) rows in ")
        end

        println(io, "$nchunks chunks:")
        println(io, "")
        show(io, top)
        if isnull(len) || get(len) > 5
            println(io, "")
            print(io, "...")
        end
    else
        println(io, "an empty DTable")
    end
end
