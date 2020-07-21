#! /usr/bin/env python

import os

import pandas as pd

metagenomes = pd.read_table("inputs/metagenomes_source-20200708.csv", sep=',', usecols=["Run"])

for metagenome in metagenomes.iterrows():
    full_path = f"/group/ctbrowngrp/irber/data/wort-data/wort-sra/sigs/{metagenome[1]['Run']}.sig"
    if os.path.exists(full_path):
        print(full_path)
