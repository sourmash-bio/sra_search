configfile: "config.yml"


rule all:
  input:
    f"outputs/{config['query_name']}.csv"


rule catalog_all:
  output: "catalogs/all_wort_sigs"
  shell: "find {config[wort_sigs]} -type f -iname '*.sig' > {output}"

rule build_rust_bin:
  output: "bin/sra_search",
  conda: "env/rust.yml"
  shell: "cargo install --git https://github.com/luizirber/phd.git --rev 455f613 sra_search --root ."

rule search:
  output: f"outputs/results/{config['query_name']}.csv"
  input:
    queries = config["query_sigs"],
    catalog = "catalogs/all_wort_sigs",
    bin = "bin/sra_search"
  params:
    threshold = config.get("threshold", 0.01),
    ksize = config.get("ksize", 31)
  shell: """
    export RAYON_NUM_THREADS={threads}
    {bin} --threshold {params.threshold} -k {params.ksize} -o {output} {input.queries} {input.catalog}
  """

rule download_signatures_from_wort:
  conda: "env/aws.yml"
  shell: """
    aws s3 sync s3://wort-sra/ {params.s3_dir} --request-payer=requester
  """
