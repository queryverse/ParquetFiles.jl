module ParquetFiles

using Parquet, IteratorInterfaceExtensions, TableTraits, NamedTuples,
    FileIO, TableShowUtils
import IterableTables

export load

struct ParquetFile
    filename::String
end

function Base.show(io::IO, source::ParquetFile)
    TableShowUtils.printtable(io, getiterator(source), "Parquet file")
end

function Base.show(io::IO, ::MIME"text/html", source::ParquetFile)
    TableShowUtils.printHTMLtable(io, getiterator(source))
end

Base.Multimedia.mimewritable(::MIME"text/html", source::ParquetFile) = true

struct ParquetNamedTupleIterator{T,T_row}
    rc::RecCursor
    nrows::Int
end

function Base.eltype(itr::ParquetNamedTupleIterator{T,T_row}) where {T,T_row}
    return T        
end

function Base.length(itr::ParquetNamedTupleIterator)
    return itr.nrows
end

function Base.start(itr::ParquetNamedTupleIterator)
    return start(itr.rc)
end

@generated function Base.next(itr::ParquetNamedTupleIterator{T,T_row}, state) where {T,T_row}
    names = fieldnames(T)
    x = quote
        v, next_state = next(itr.rc, state)
        return T($([T.types[i]<:String ? :(String(copy(v.$(names[i])))) : :(v.$(names[i])) for i=1:length(T.types)]...)), next_state
    end
    return x
end

function Base.done(itr::ParquetNamedTupleIterator, state)
    return done(itr.rc, state)
end

function fileio_load(f::FileIO.File{FileIO.format"Parquet"})
    return ParquetFile(f.filename)
end

IteratorInterfaceExtensions.isiterable(x::ParquetFile) = true
TableTraits.isiterabletable(x::ParquetFile) = true

function IteratorInterfaceExtensions.getiterator(file::ParquetFile)
    p = ParFile(file.filename)

    T_row_name = Symbol("RCType$(String(gensym())[3:end])")

    schema(JuliaConverter(ParquetFiles), p, T_row_name)

    T_row = eval(T_row_name)

    col_names = fieldnames(T_row)
    col_types = [i<:Vector{UInt8} ? String : i for i in T_row.types]

    T = eval(:(@NT($(col_names...)))){col_types...}

    rc = RecCursor(p, 1:nrows(p), colnames(p), JuliaBuilder(p, T_row))

    it = ParquetNamedTupleIterator{T,T_row}(rc, nrows(p))
    
    return it
end

end # module
