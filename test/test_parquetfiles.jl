@testitem "ParquetFiles" begin
    using DataValues
    using IteratorInterfaceExtensions
    using TableTraits

    test_filename = joinpath(@__DIR__, "data", "nation.impala.parquet")

    pqf = load(test_filename)

    @test IteratorInterfaceExtensions.isiterable(pqf) == true
    @test TableTraits.isiterabletable(pqf) == true

    it = IteratorInterfaceExtensions.getiterator(pqf)

    ar = collect(it)

    @test length(ar) == 25
    @test isequal(ar[1], (n_nationkey=DataValue(Int32(0)), n_name=DataValue("ALGERIA"), n_regionkey=DataValue(Int32(0)), n_comment=DataValue(" haggle. carefully final deposits detect slyly agai")))

    @test collect(pqf) == ar

    @test first(sprint((stream, data) -> show(stream, "text/html", data), pqf), 100) == "<table><thead><tr><th>n_nationkey</th><th>n_name</th><th>n_regionkey</th><th>n_comment</th></tr></th"
    @test first(sprint((stream, data) -> show(stream, "application/vnd.dataresource+json", data), pqf), 100) == "{\"schema\":{\"fields\":[{\"name\":\"n_nationkey\",\"type\":\"integer\"},{\"name\":\"n_name\",\"type\":\"string\"},{\"nam"
    @test first(sprint(show, pqf), 100) == "25x4 Parquet file\nn_nationkey │ n_name      │ n_regionkey\n────────────┼─────────────┼────────────\n0 "
    @test showable("text/html", pqf) == true
    @test showable("application/vnd.dataresource+json", pqf) == true
end

@testitem "Column interface" begin
    using TableTraits

    pqf = load(joinpath(@__DIR__, "data", "nation.impala.parquet"))

    @test TableTraits.supports_get_columns_copy_using_missing(pqf) == true

    data = TableTraits.get_columns_copy_using_missing(pqf)

    @test keys(data) == (:n_nationkey, :n_name, :n_regionkey, :n_comment)
    @test length(data.n_nationkey) == 25
    @test data.n_nationkey[1:3] == Int32[0, 1, 2]
    @test data.n_name[1:3] == ["ALGERIA", "ARGENTINA", "BRAZIL"]

    # Byte array columns without a UTF8 annotation are still presented as strings.
    @test eltype(data.n_name) == Union{Missing,String}
end

@testitem "File handles are released" begin
    # Parquet2 memory maps by default; ParquetFiles must materialize the columns so the
    # file is not left locked (this fails on Windows if a mapping outlives the read).
    tmp = tempname() * ".parquet"
    cp(joinpath(@__DIR__, "data", "nation.impala.parquet"), tmp)

    ar = collect(load(tmp))
    @test length(ar) == 25

    rm(tmp)
    @test !isfile(tmp)
end
