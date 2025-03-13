"""
Work on a framework that receives multiple texts (“document” even if they are short),
 a query that is very degenerated now,
 and its output is getting the GoLLIE format before the postprocess

Take one of the datasets

First step: receive texts, a few shots of the made-up question,
try to get from the model what its naive output for this, try different questions
Try 100 texts with the same query (100 different runs), and we will have 100 (different?) schemas predicted
Write the code as modular as possible, to be able to work on much longer documents, larger scale etc all will work.

"""

from typing import List
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
import torch
from tqdm import tqdm
import json
import re
import argparse
import os

access_token = "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"


class Input:
    def __init__(self, query: str, text: str, result: List, explanation: str = None):
        self.query = query
        self.text = text
        self.result = result
        self.explanation = explanation

    def __repr__(self):
        explanation = f", explanation={self.explanation}" if self.explanation is not None else ""
        return (f"Input(query={self.query!r}, "
                f"text={self.text!r}, "
                f"result={self.result!r})")


few_shots = [Input(query="Does the trip cost lots of money?",
                   text="A five-day road trip for two in Tuscany features light hikes, "
                        "wine tasting, and rejuvenating natural hot springs—all within a"
                        " manageable drive from the city.",
                   result=["Duration", "Number of Participants", "Place", "Attraction"],
                   explanation="all found in the text: Duration: \"five-day\", "
                               "Number of Participants: \"Two\", Place: \"Tuscany\", "
                               "Attraction: [\"light hikes\", \"wine tasting\", \"hot springs\"] "
                               "And all can affect the costs."),
             Input(query="Who will win the game?",
                   text="Next Sunday, Barcelona and Real Madrid will face off in a highly "
                        "anticipated soccer match that promises intense rivalry and thrilling action.",
                   result=["Sport", "Time", "Team"],
                   explanation="all found in the text: Sport: \"soccer\",  Time: \"Next Sunday\", "
                               "Team: [\"Barcelona\", \"Real Madrid\" And all can affect the winner.")
             ]

few_shots_chained = ""
for n, i in enumerate(few_shots):
    few_shots_chained += f"{n + 1}. Query: {i.query}\nText: {i.text}\nFields: {i.result!s}\n"


def create_prompt_one_doc(cur_query, cur_data):
    final_input = Input(cur_query, cur_data, [])
    prompt = f"You are given a text and a query. Your task is to identify the set of fields present in the text that" \
             f" can directly address the query. You should only output an array (e.g., [field1, field2, ...]) " \
             f"of the appropriate fields without any explanation or additional commentary. \n" \
             f"Few-shot examples: \n" \
             f"{few_shots_chained} \n" \
             f"Now answer the following without providing any explanation: \n" \
             f"Query: {final_input.query}\nText: {final_input.text}\n"

    print(prompt)
    return prompt


def create_prompt_multi_doc(cur_query, cur_data):
    final_input = Input(cur_query, cur_data, [])
    prompt = f"You are given a multiple documents and a query. " \
             f"Your task is to identify the set of fields present in the text that" \
             f" can directly address the query. It can be that not all of them can be found in all of the texts" \
             f"You should only output one array (e.g., [field1, field2, ...]) " \
             f"of the appropriate fields without any explanation or additional commentary. \n" \
             f"Few-shot examples: \n" \
             f"{few_shots_chained} \n" \
             f"Now answer the following without providing any explanation: \n" \
             f"Query: {final_input.query}\nText: {final_input.text}\n"

    print(prompt)
    return prompt


def write_to_log(message, log_file):
    print(message)
    with open(log_file, "a") as file:
        file.write(str(message) + "\n")


def run_model(data, query, log_file, num_documents=1, model_name="meta-llama/Llama-3.2-3B-Instruct", quantize=False):
    quantization_config = BitsAndBytesConfig(load_in_4bit=True, bnb_4bit_compute_dtype=torch.bfloat16) \
        if quantize else None
    model = AutoModelForCausalLM.from_pretrained(model_name,
                                                 token=access_token,
                                                 quantization_config=quantization_config).cuda()
    tokenizer = AutoTokenizer.from_pretrained(model_name, token=access_token)

    # Make sure pad_token is set correctly
    tokenizer.pad_token = tokenizer.eos_token
    model.config.pad_token_id = tokenizer.eos_token_id

    def get_tokenized_list(documents, cur_query):
        tokenized_list = []
        for cur_data in documents:
            prompt = create_prompt_one_doc(cur_query, cur_data) \
                if num_documents == 1 \
                else create_prompt_multi_doc(cur_query, cur_data)
            inputs = tokenizer(
                prompt,
                return_tensors="pt",
                return_attention_mask=True,
                padding=True,
                truncation=True
            )
            tokenized_list.append(inputs)
        return tokenized_list

    tokenized_list = get_tokenized_list(data, query)
    progress_bar = tqdm(tokenized_list, desc="Inference: ")

    write_to_log(f"-----------------Inference in {model_name} model: ", log_file)

    for i, t in enumerate(progress_bar):
        input_data = {k: v.cuda() for k, v in t.items()}
        model_output = model.generate(**input_data)
        decoded_text = tokenizer.batch_decode(model_output)[0]

        # Create a dictionary containing query, data, and the answer
        cur_output_dict = {
            "query": query,
            "data": data[i],
            "answer": decoded_text
        }
        write_to_log(str(cur_output_dict), log_file)


def get_data(json_path, max_lines=None):
    # getting data from GoLLIE data generated
    pattern_text = re.compile(r'text\s*=\s*"(.*?)"')
    pattern_schema = re.compile(r"class\s+(\w+)(?:\(.*?\))?:", re.MULTILINE)

    data = []
    original_schema = []
    j = 0

    with open(json_path, 'r', encoding='utf-8') as f:
        for line in f:
            if max_lines is not None and j > max_lines: break
            # Each line in the file is a separate JSON object
            json_obj = json.loads(line)

            code_snippet = json_obj.get('text', '')
            match_data = pattern_text.search(code_snippet)
            if match_data:
                data.append(match_data.group(1))

            match_schema = pattern_schema.findall(code_snippet)
            for match in match_schema:
                if match not in original_schema:
                    original_schema.append(match)

            j += 1
    name_no_ext, ext = os.path.splitext(json_path)

    metadata = {
        "domain": "",
        "dataset": name_no_ext,
        "original schema": original_schema  # for example: ["Location", "Organization", "Person", "Miscellaneous"]
    }

    return data, metadata


def main(args):
    input_path = args.input
    output_path = args.output

    if os.path.exists(output_path):
        print(f"removed {output_path}")
        os.remove(output_path)

    query = "what is happening in the scene?"
    data, metadata = get_data(input_path, args.max_n)

    write_to_log("-----------------Metadata:", output_path)
    write_to_log(str(metadata), output_path)

    run_model(data=data, query=query, log_file=output_path, num_documents=args.num_of_docs)


if __name__ == "__main__":
    print("start")
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i",
        "--input",
        dest="input",
        type=str,
        required=True,
        help="Path to the input IE data JSON file.",
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        type=str,
        required=True,
        help="Path where output will be saved.",
    )
    parser.add_argument(
        "-max_n",
        dest="max_n",
        type=int,
        help="Maximum number of examples to process.",
    )
    parser.add_argument(
        "-num_of_docs",
        dest="num_of_docs",
        type=int,
        help="Number of documents to process each time.",
    )
    args = parser.parse_args()

    main(args)
