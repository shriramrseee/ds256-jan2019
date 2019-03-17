from data_load.loaders import load_fact, load_date, load_literal

remote = "ws://10.24.24.2:8182/gremlin"
local = "ws://localhost:8182/gremlin"

# Load into Cloud server
load_fact(remote, "~/yagoFacts.tsv")
load_date(remote, "~/yagoDateFacts.tsv")
load_literal(remote, "~/yagoLiteralFacts.tsv")


# Test
# load_fact(local, "../local_data/yagoFacts.txt")
# load_date(local, "../local_data/yagoDateFacts.txt")
# load_literal(local, "../local_data/yagoLiteralFacts.txt")
