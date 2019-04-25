from data_load.loaders import load_fact, load_date, load_literal

remote = "ws://35.200.133.241:8182/gremlin"
local = "ws://localhost:8182/gremlin"

# Load into Cloud server
load_fact(remote, "../local_data/yagoFacts.tsv")
load_date(remote, "../local_data/yagoDateFacts.tsv")
load_literal(remote, "../local_data/yagoLiteralFacts.tsv")

# Test
# load_fact(local, "../local_data/yagoFacts.tsv")
# load_date(local, "../local_data/yagoDateFacts.tsv")
# load_literal(local, "../local_data/yagoLiteralFacts.tsv")







