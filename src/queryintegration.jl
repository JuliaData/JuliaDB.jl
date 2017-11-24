function QueryOperators.query(source::IndexedTables.NextTable)
    return source
end

function QueryOperators.map(source::IndexedTables.NextTable, f_as_anon, f_as_expr)
    # TODO Anaylze f_as_expr to find out which columns are actually used,
    # then pass those column names as the select argument
    return map(f_as_anon, source)
end

function QueryOperators.filter(source::IndexedTables.NextTable, f_as_anon, f_as_expr)
    # TODO Anaylze f_as_expr to find out which columns are actually used,
    # then pass those column names as the select argument    
    return filter(f_as_anon, source)
end
