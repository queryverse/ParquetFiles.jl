using Documenter, ParquetFiles

makedocs(
	modules = [ParquetFiles],
	sitename = "ParquetFiles.jl",
	analytics = "UA-132838790-1",
	pages = [
        "Introduction" => "index.md"
    ]
)

deploydocs(
    repo = "github.com/queryverse/ParquetFiles.jl.git"
)
