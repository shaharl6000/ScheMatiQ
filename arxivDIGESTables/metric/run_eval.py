from argparse import ArgumentParser
import json
from tqdm import tqdm
import os
from metrics import SchemaRecallMetric
from metrics_utils import (
    BaseFeaturizer,
    DecontextFeaturizer,
    ExactMatchScorer,
    JaccardAlignmentScorer,
    Llama3AlignmentScorer,
    SentenceTransformerAlignmentScorer,
    ValueFeaturizer,
)
from table import Table

MINE = True

def open_gold_tables(tables_path):
    """
    Returns a mapping from tabid to gold Table objects
    """

    tabid_to_gold_table = {}
    with open(tables_path, encoding="utf-8") as f:
        for line in f:
            table_dict = json.loads(line)
            tabid = table_dict["tabid"]
            table = Table(
                tabid=tabid,
                schema=list(json.loads(table_dict['table']).keys()),
                values=json.loads(table_dict['table']),
                caption= "" #table_dict["caption"], TODO sl : caption is needed?
            )
            tabid_to_gold_table[tabid] = table
    return tabid_to_gold_table

def open_pred_tables(tables_path):

    if not MINE:
        filter_jsonl_path = r"C:\Users\shaharl\Desktop\shahar\Uni\QueryDiscovery\data\schema_6_filtered\arxiv_tables_filtered_4_columns_queries_070725.jsonl"

        # Load the set of tabids to keep
        with open(filter_jsonl_path, encoding="utf-8") as f:
            allowed_tabids = {json.loads(line)["tabid"] for line in f}

    pred_tables = []
    with open(tables_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            line = line.replace("```json", "").replace("```", "")
            if not line:
                continue
            try:
                table_dict = json.loads(line)
            except json.decoder.JSONDecodeError:
                print("Error decoding line {}".format(line))
                continue

            if not MINE:
                if table_dict["metadata"]["tabid"] not in allowed_tabids:
                    continue
                pred_table = Table(
                    tabid=table_dict["metadata"]["tabid"],
                    schema=list(table_dict["table"].keys()),
                    values=(table_dict["table"].keys())
                )
            else:
                # sl: for my generated tables
                pred_table = Table(
                    tabid=table_dict["tabid"],
                    schema=table_dict["aspects"],
                    values=table_dict["aspects"]  # TODO sl: no need for values..
                )

            table_dict["table_cls"] = pred_table
            pred_tables.append(table_dict)

    return pred_tables

def load_featurizer(featurizer_name):
    if featurizer_name == "name":
        return BaseFeaturizer("name")
    elif featurizer_name == "values":
        return ValueFeaturizer("values")
    elif featurizer_name == "decontext":
        return DecontextFeaturizer("decontext", model="mistralai/Mixtral-8x7B-Instruct-v0.1")
    else:
        raise ValueError(f"Unknown featurizer name: {featurizer_name}.")

def load_scorer(scorer_name):
    if scorer_name == "exact_match":
        return ExactMatchScorer()
    elif scorer_name == "jaccard":
        return JaccardAlignmentScorer(remove_stopwords=True)
    elif scorer_name == "sentence_transformers":
        return SentenceTransformerAlignmentScorer()
    elif scorer_name == "llama3":
        return Llama3AlignmentScorer()

import numpy as np
def to_serializable(obj):
    if isinstance(obj, (np.floating, np.integer)):
        return obj.item()          # scalar → native Python type
    if isinstance(obj, np.ndarray):
        return obj.tolist()        # array  → list
    try:
        return str(obj)
    except Exception:
        return f"<<non-serializable: {type(obj).__qualname__}>>"
    # raise TypeError(f"{obj!r} is not JSON-serialisable")

def main(gold_tables, pred_tables, out_file, 
         featurizer="name", scorer="sentence_transformers",
         threshold=0.7):
    # open gold and predicted tables
    tabid_to_gold_table = open_gold_tables(gold_tables)
    pred_tables = open_pred_tables(pred_tables)

    # load the metric
    featurizer = load_featurizer(featurizer)
    scorer = load_scorer(scorer)
    metric = SchemaRecallMetric(featurizer=featurizer, alignment_scorer=scorer, sim_threshold=threshold)

    # run the evaluation
    results = []
    for pred_table_instance in tqdm(pred_tables):
        pred_table = pred_table_instance.pop("table_cls")

        if len(pred_table_instance["aspects"]) == 0:
            print(f"W: problem with EMPTY schema! ")
            continue

        if len(pred_table_instance["aspects"]) == 3 and "rational" in pred_table_instance["aspects"][2]:
            aspects = pred_table_instance["aspects"]
            print(f"W: problem with schema! {aspects}")
            continue

        gold_table = tabid_to_gold_table[pred_table.tabid]
        recall, _, alignment = metric.add(pred_table, gold_table, return_scores=True)
        # print(f"Recall: {recall}, alignment: {alignment}")
        alignment_str_keys = dict(zip(map(str, alignment), alignment.values()))
        results.append(pred_table_instance | {"scores": {"recall": recall, "alignment": alignment_str_keys, "featurizer": featurizer, "scorer": scorer, "threshold": threshold}})
    
    # write the results to disk
    with open(out_file, "w", encoding="utf-8") as f:
        for r in results:
            try:
                f.write(json.dumps(r, default=to_serializable) + "\n")
            except Exception as e:
                print(f"Error writing result: {r}")
                print(f"Exception: {e}")
                continue

        
    scores_dict = metric.process_scores()
    print(f"scores_dict for {out_file} is: {scores_dict}")




if __name__ == "__main__":
    
    # argp = ArgumentParser()
    # argp.add_argument("--gold_tables", type=str)
    # argp.add_argument("--pred_tables", type=str)
    # argp.add_argument("--out_file", type=str)
    # argp.add_argument("--featurizer", type=str, default="decontext", choices=["name", "values", "decontext"], help="name: uses the column name; values: concatenates the column name with the column's values; decontext: decontextualizes the column name using the values as context.")
    # argp.add_argument("--scorer", type=str, default="sentence_transformers", choices=["exact_match", "jaccard", "sentence_transformers", "llama3"])
    # argp.add_argument("--threshold", type=float, default=0.7, help="Threshold used to determine a match for exact_match, jaccard and sentence_transformer scorers")
    # argp.add_argument("--eval_type", type=str, default="schema", choices=["schema", "values"])
    # args = argp.parse_args()

    pred_directory = r"../../QueryDiscovery/data/schema_6_filtered" if MINE else "../predictions"
    out_directory = r"../predictions"

    prediction_files = ["results_gpt4o_clean_QBSD"]

    featurizer = "name"
    scorer = "sentence_transformers"
    thresholds = [0.5]

    for cur_pred in prediction_files:
        for thresh in thresholds:
            main(
                pred_tables=os.path.join(pred_directory, f"{cur_pred}.jsonl"),
                gold_tables=os.path.join(pred_directory,
                                         f"arxiv_tables_filtered_4_columns_retrieved_queries_270725.jsonl"),
                out_file=os.path.join(out_directory, f"SCORE_{cur_pred}_{featurizer}_{scorer}_{thresh}.jsonl"),
                featurizer=featurizer,
                scorer=scorer,
                threshold=thresh
            )