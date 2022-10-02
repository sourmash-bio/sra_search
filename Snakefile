configfile: "config.yml"

import asyncio
import os
import shutil

#import logging
#logging.basicConfig(level=logging.DEBUG)

import aiofiles
import httpx
from snakemake.common import async_run
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider

HTTP = HTTPRemoteProvider()

rule all:
  input:
    f"outputs/results/{config['query_name']}.csv"

#########################################################
# Rules for input preparation:
#  - Download RunInfo data from SRA
#  - Download all sigs from wort (if possible)
#  - Prepare a local catalog (a file with paths to sigs) for the searcher
#########################################################

rule download_source:
  output: config['sources']
  input:
    HTTP.remote('trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi',
                additional_request_string='?save=efetch&db=sra&rettype=runinfo&term="METAGENOMIC"[Source] NOT amplicon[All Fields] AND cluster_public[prop]',
                keep_local=True)
  run:
    shell("mv {input:q} {output}")

rule catalog:
  output:
    catalog=f"outputs/catalogs/{os.path.basename(config['sources'])}"
  input:
    runinfo=config['sources']
  run:
    import csv
    from pathlib import Path

    os.makedirs(config['wort_sigs'], exist_ok=True)

    ##################################
    # step 1: find what SRA IDs to download
    ##################################
    sraids = set()
    with open(input.runinfo) as fp:
      data = csv.DictReader(fp, delimiter=',')
      for dataset in data:
        sraids.add(dataset['Run'])
    print(f"step 1: {len(sraids)}")

    ##################################
    # step 2: find what sigs are already downloaded
    ##################################
    sig_paths = set()
    sraids_to_download = set()
    for sraid in sraids:
      sig_path = Path(config['wort_sigs']) / f"{sraid}.sig"
      if sig_path.exists():
        sig_paths.add(sig_path)
      else:
        sraids_to_download.add(sraid)
    del sraids
    print(f"step 2: {len(sraids_to_download)}")

    ##################################
    # step 3: download sigs from wort
    ##################################
    async def download_sig(sraid, client):
      response = await client.get(f"https://wort.sourmash.bio/v1/view/sra/{sraid}")
      # verify if sig exists in wort
      if not (response.is_redirect or response.is_success):
        return None

      ## TODO: wort currently redirects to IPFS, but need to check
      ## here and act accordingly instead of assuming
      request = response.next_request
      async with client.stream('GET', request.url) as response:
        response.raise_for_status()
        # download to temp location
        async with aiofiles.tempfile.NamedTemporaryFile(delete=False) as f:
          async for chnk in response.aiter_raw():
            await f.write(chnk)
          await f.flush()

          # move to final location
          ## TODO: the goal here is to avoid incomplete downloads,
          ## but I'm still getting incomplete files =/
          sig_path = Path(config['wort_sigs']) / f"{sraid}.sig"
          await asyncio.to_thread(shutil.copyfile, f.name, sig_path)
          return sig_path

    async def collect():
      async with httpx.AsyncClient(timeout=30.0,
                                   limits=httpx.Limits(max_connections=config['max_downloaders'])) as client:
        tasks = [
          download_sig(sraid, client) for sraid in sraids_to_download
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
      return results

    if not config.get('skip_download', True):
      # TODO: deal with errors
      results = asyncio.run(collect())
      print(f"step 3: {len(results)}")
      for result in results:
        if result is None:
          # Couldn't find a sig in wort, just skip
          pass
        elif isinstance(result, BaseException):
          # catch-all exception for now, need to figure out what to do
          # probably retry?
          print(f"exception: {result}")
          raise result
        elif isinstance(result, str):
          # valid path!
          sig_paths.add(sig_path)

    ##################################
    # step 4: prepare catalog
    ##################################
    with open(output[0], 'w') as fout:
      for sig_path in sig_paths:
        if sig_path.exists():
          fout.write(f"{sig_path}\n")

#########################################################
# searcher-related rules
#  - Compile the searcher (a rust binary)
#  - Execute the searcher
#########################################################

rule build_rust_bin:
  output: "bin/searcher",
  input:
    src="searcher/src/main.rs",
    deps="searcher/Cargo.toml",
    deps_lock="searcher/Cargo.lock",
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
    ksize = config.get("ksize", 31),
    scaled = config.get("scaled", 1000),
  threads: 32
  shell: """
    export RAYON_NUM_THREADS={threads}
    set +e
    {input.bin} \
        --threshold {params.threshold} \
        -k {params.ksize} \
        --scaled {params.scaled} \
        -o {output} \
        {input.queries} \
        {input.catalog}
    exit 0
  """

#########################################################
# Future: searcher results post-processsing
#########################################################
