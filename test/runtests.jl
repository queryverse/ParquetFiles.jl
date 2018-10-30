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

end
