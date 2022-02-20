#! /usr/bin/env python

data = pd.read_table("results.csv", sep=",", names=["MAG", "metagenome", "containment"])

data['metagenome'] = data['metagenome'].str.replace('/data/wort/wort-sra/sigs/', '')
data[data['containment'] > 0.5].sort_values(by="containment")
