module ParquetFiles

using Parquet, IteratorInterfaceExtensions, TableTraits, FileIO
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
Base.Multimedia.showable(::MIME"text/html", source::ParquetFile) = true

function Base.show(io::IO, ::MIME"application/vnd.dataresource+json", source::ParquetFile)
    TableShowUtils.printdataresource(io, getiterator(source))
end
Base.Multimedia.showable(::MIME"application/vnd.dataresource+json", source::ParquetFile) = true

function fileio_load(f::FileIO.File{FileIO.format"Parquet"})
    return ParquetFile(f.filename)
end

IteratorInterfaceExtensions.isiterable(x::ParquetFile) = true
TableTraits.isiterabletable(x::ParquetFile) = true

function IteratorInterfaceExtensions.getiterator(file::ParquetFile)
    p = ParFile(file.filename; map_logical_types=true)
    return RecordCursor(p)
end

end # module
