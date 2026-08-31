module ParquetFiles

using Parquet2, IteratorInterfaceExtensions, TableTraits, TableTraitsUtils, Tables, FileIO
import IterableTables, DataValues, TableShowUtils

export load, File, @format_str

struct ParquetFile
    filename::String
end

function Base.show(io::IO, source::ParquetFile)
    TableShowUtils.printtable(io, getiterator(source), "Parquet file")
end

function Base.show(io::IO, ::MIME"text/html", source::ParquetFile)
    TableShowUtils.printHTMLtable(io, getiterator(source))
end
Base.showable(::MIME"text/html", source::ParquetFile) = true

function Base.show(io::IO, ::MIME"application/vnd.dataresource+json", source::ParquetFile)
    TableShowUtils.printdataresource(io, getiterator(source))
end
Base.showable(::MIME"application/vnd.dataresource+json", source::ParquetFile) = true

function fileio_load(f::FileIO.File{FileIO.format"Parquet"})
    return ParquetFile(f.filename)
end

IteratorInterfaceExtensions.isiterable(x::ParquetFile) = true
TableTraits.isiterabletable(x::ParquetFile) = true
TableTraits.supports_get_columns_copy_using_missing(x::ParquetFile) = true

# Byte array columns that carry no UTF8 annotation (anything written before the logical
# type existed) come back as `Vector{UInt8}` rather than `String`, so they are decoded here
# the way this package has always presented them.
_materialize(c, ::Type{T}) where {T} = convert(Vector{T}, c)

_materialize(c, ::Type{T}) where {T<:AbstractVector{UInt8}} = String[String(copy(x)) for x in c]

function _materialize(c, ::Type{Union{Missing,T}}) where {T<:AbstractVector{UInt8}}
    return Union{Missing,String}[x === missing ? missing : String(copy(x)) for x in c]
end

# Reads the file eagerly into plain `Vector`s. Parquet2 memory maps by default and its
# string and dictionary columns are views onto that buffer, so the columns are materialized
# before the dataset is closed: otherwise the mapping outlives `ds` and keeps the file
# locked on Windows.
function _loaddata(file::ParquetFile)
    ds = Parquet2.Dataset(file.filename; use_mmap=false)
    try
        cols = Tables.columns(ds)
        names = Symbol[Symbol(n) for n in Tables.columnnames(cols)]
        columns = Any[_materialize(c, eltype(c)) for c in (Tables.getcolumn(cols, n) for n in names)]
        return columns, names
    finally
        close(ds)
    end
end

function IteratorInterfaceExtensions.getiterator(file::ParquetFile)
    columns, names = _loaddata(file)

    return TableTraitsUtils.create_tableiterator(columns, names)
end

function TableTraits.get_columns_copy_using_missing(file::ParquetFile)
    columns, names = _loaddata(file)

    return NamedTuple{(names...,)}((columns...,))
end

function Base.collect(x::ParquetFile)
    return collect(getiterator(x))
end

end # module
