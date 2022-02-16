#!/bin/bash -login
#SBATCH -J sra_search
#SBATCH --time=100:00:00
#SBATCH -N 1 -n 1 -c 32 
#SBATCH -p bmm 
#SBATCH --mem=20GB
#SBATCH -A ctbrowngrp
#SBATCH -o logs/%j.out
##SBATCH -L umask=027

cd $SLURM_SUBMIT_DIR

source ~/.bashrc
conda activate sra_search

set -o nounset
set -o errexit
set -x

snakemake -j 32 --use-conda -p

echo ${SLURM_JOB_NODELIST}       # Output Contents of the SLURM NODELIST

env | grep SLURM            # Print out values of the current jobs SLURM environment variables

scontrol show job ${SLURM_JOB_ID}     # Print out final statistics about resource uses before job exits
