using ParquetFiles
using Parquet
using IteratorInterfaceExtensions
using TableTraits
using Test

@testset "ParquetFiles" begin

    parquet_pkg_dir = joinpath(dirname(pathof(Parquet)), "..")

    include(joinpath(parquet_pkg_dir, "test", "get_parcompat.jl"))

    test_filename = joinpath(parquet_pkg_dir, "test", "parquet-compatibility", "parquet-testdata", "impala", "1.1.1-NONE", "nation.impala.parquet")

    pqf = load(test_filename)

    @test IteratorInterfaceExtensions.isiterable(pqf) == true
    @test TableTraits.isiterabletable(pqf) == true

    it = IteratorInterfaceExtensions.getiterator(pqf)

    ar = collect(it)

    @test length(ar) == 25
    @test ar[1] == (n_nationkey = 0, n_name = "ALGERIA", n_regionkey = 0, n_comment = " haggle. carefully final deposits detect slyly agai")

    @test sprint((stream, data)->show(stream, "text/html", data), pqf)[1:100] == "<table><thead><tr><th>n_nationkey</th><th>n_name</th><th>n_regionkey</th><th>n_comment</th></tr></th"
    @test sprint((stream, data)->show(stream, "application/vnd.dataresource+json", data), pqf)[1:100] == "{\"schema\":{\"fields\":[{\"name\":\"n_nationkey\",\"type\":\"string\"},{\"name\":\"n_name\",\"type\":\"string\"},{\"name"
    @test sprint(show, pqf)[1:100] == "25x4 Parquet file\nn_nationkey │ n_name    │ n_regionkey\n────────────┼─"
    @test showable("text/html", pqf) == true
    @test showable("application/vnd.dataresource+json", pqf) == true

end
