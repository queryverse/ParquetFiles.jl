using Documenter, ParquetFiles

makedocs(
	modules=[ParquetFiles],
	sitename="ParquetFiles.jl",
	format = Documenter.HTML(analytics = "UA-132838790-1"),
	warnonly = [:missing_docs],
	pages=[
        "Introduction" => "index.md"
    ]
)

deploydocs(
    repo="github.com/queryverse/ParquetFiles.jl.git"
)
