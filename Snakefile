configfile: "config.yml"

import os

from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider
HTTP = HTTPRemoteProvider()

rule all:
  input:
    f"outputs/results/{config['query_name']}.csv"

rule download_source:
  output: config['sources']
  input:
    HTTP.remote('trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi',
                additional_request_string='?save=efetch&db=sra&rettype=runinfo&term="METAGENOMIC"[Source] NOT amplicon[All Fields] AND cluster_public[prop]',
                keep_local=True)
  run:
    shell("mv {input:q} {output}")

rule catalog_metagenomes:
  output: f"outputs/catalogs/{os.path.basename(config['sources'])}"
  input: config['sources']
  run:
    import csv
    from pathlib import Path

    with open(input[0]) as fp:
      data = csv.DictReader(fp, delimiter=',')
      with open(output[0], 'w') as fout:
        for dataset in data:
          sraid = dataset['Run']
          sig_path = Path(config['wort_sigs']) / f"{sraid}.sig"
          if sig_path.exists():
            fout.write(f"{sig_path}\n")

rule build_rust_bin:
  output: "bin/searcher",
  conda: "env/rust.yml"
  shell: "cargo install --path searcher --root ."

rule search:
  output: f"outputs/results/{config['query_name']}.csv"
  input:
    queries = config["query_sigs"],
    catalog = f"outputs/catalogs/{os.path.basename(config['sources'])}",
    bin = "bin/searcher"
  params:
    threshold = config.get("threshold", 0.01),
    ksize = config.get("ksize", 31)
  threads: 32
  shell: """
    export RAYON_NUM_THREADS={threads}
    set +e
    {input.bin} --threshold {params.threshold} -k {params.ksize} -o {output} {input.queries} {input.catalog}
    exit 0
  """
