
storage:
    provider="fsspec",
    protocol="file",

# Example rule that uses the storage plugin
rule example:
    input:
        storage.fsspec("input.txt"),
    output:
        storage.fsspec("output.txt"),
    shell:
        "cp {input} {output}"

# Example with wildcards
rule wildcard_example:
    input:
        storage.fsspec("inputs/{sample}.txt"),
    output:
        storage.fsspec("outputs/{sample}.processed.txt"),
    shell:
        "cp {input} {output}"

# Example with multiple inputs
rule multiple_inputs:
    input:
        storage.fsspec("inputs/sample1.txt"),
        storage.fsspec("inputs/sample2.txt"),
    output:
        storage.fsspec("outputs/combined.txt"),
    shell:
        "cat {input} > {output}"
