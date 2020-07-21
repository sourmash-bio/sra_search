configfile: "config.yml"


def result_for_sig(w):
    with open('catalog') as f:
        all_sigs = f.read().splitlines()
    for sig in all_sigs:
        out = os.path.basename(sig)[:-4]
        yield f"outputs/results/{config['query_name']}/{out}.out"


rule all:
  input:
    f"outputs/{config['query_name']}.csv"


#rule catalog:
#  output: "catalog"
#  shell: "find {config[wort_sigs]} -type f > {output}"


rule summary:
  output: f"outputs/{config['query_name']}.csv"
  input:
    "catalog",
    result_for_sig
  shell: """
    find outputs/results/{config[query_name]} -type f | \
      parallel -j4 wc -l | \
      grep -v "^1 " | \
      cut -d " " -f 2 | \
      xargs cat | \
      sort -n > {output}
  """


rule containment:
  output: f"outputs/results/{config['query_name']}/{{sra_id}}.out"
  input: 
    query = config["query_sig"],
    sig = f"{config['wort_sigs']}/{{sra_id}}.sig"
  conda: "env/sourmash.yml"
  shell: """
    sourmash search -o {output} \
        --containment \
        --scaled 1000 \
        --threshold 0.1 \
        -k 31 \
        {input.query} \
        {input.sig}
  """
