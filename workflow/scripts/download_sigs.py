import asyncio
import csv
import os
from pathlib import Path
import shutil

import aiofiles
import httpx
from snakemake.common import async_run

os.makedirs(snakemake.config['wort_sigs'], exist_ok=True)

##################################
# step 1: find what SRA IDs to download
##################################
sraids = set()
with open(snakemake.input.runinfo) as fp:
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
  sig_path = Path(snakemake.config['wort_sigs']) / f"{sraid}.sig"
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
      sig_path = Path(snakemake.config['wort_sigs']) / f"{sraid}.sig"
      await asyncio.to_thread(shutil.copyfile, f.name, sig_path)
      return sig_path

async def collect():
  async with httpx.AsyncClient(timeout=30.0,
                               limits=httpx.Limits(max_connections=snakemake.config['max_downloaders'])) as client:
    tasks = [
      download_sig(sraid, client) for sraid in sraids_to_download
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
  return results

if not snakemake.config.get('skip_download', True):
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
with open(snakemake.output[0], 'w') as fout:
  for sig_path in sig_paths:
    if sig_path.exists():
      fout.write(f"{sig_path}\n")
