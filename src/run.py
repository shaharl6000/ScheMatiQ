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


def create_prompt(instructions, cur_query, docs):
    """
    Create a prompt from instructions, the query, and a list of documents.
    If docs is a list of strings, we combine them into a single prompt.
    """
    # Combine all documents into one text block
    # You can style this however you want; here's one example:
    docs_text = ""
    for i, doc in enumerate(docs, start=1):
        docs_text += f"Document {i}: {doc}\n"

    final_input = Input(cur_query, docs_text, [])
    prompt = (
        f"{instructions}:\n"
        f"{few_shots_chained}\n"
        "Now answer the following without providing any explanation:\n"
        f"Query: {final_input.query}\n"
        f"{final_input.text}\n"
    )
    return prompt


def write_to_log(message, log_file):
    print(message)
    with open(log_file, "a") as file:
        file.write(str(message) + "\n")


def run_model(
        instructions,
        data,  # List of documents
        query,
        log_file,
        num_documents=1,  # Can be an integer or None
        model_name="meta-llama/Llama-3.2-3B-Instruct",
        quantize=False
):
    quantization_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_compute_dtype=torch.bfloat16
    ) if quantize else None
    model = AutoModelForCausalLM.from_pretrained(model_name,
                                                 token=access_token,
                                                 quantization_config=quantization_config,
                                                 device_map="auto",
                                                 )
    tokenizer = AutoTokenizer.from_pretrained(model_name, token=access_token)

    # Make sure pad_token is set correctly
    tokenizer.pad_token = tokenizer.eos_token
    model.config.pad_token_id = tokenizer.eos_token_id
    max_context_length = int(model.config.max_position_embeddings / 2)

    def make_chunks(documents, chunk_size, max_length):
        """
        If chunk_size is not None, slice the documents into groups
        of that size, applying truncation at the tokenizer level.

        If chunk_size is None, keep adding docs until we exceed
        the model's max context length, then start a new prompt.
        """
        if chunk_size is not None:
            # Fixed-size chunks
            for i in range(0, len(documents), chunk_size):
                yield documents[i: i + chunk_size]
        else:
            # Greedy grouping until we reach max context length
            current_batch = []
            for doc in documents:
                test_batch = current_batch + [doc]
                # Create a prompt to see if we exceed max length
                test_prompt = create_prompt(instructions, query, test_batch)
                test_ids = tokenizer(
                    test_prompt,
                    return_tensors="pt",
                    truncation=False,  # We'll do a strict check ourselves
                    padding=False
                )["input_ids"]

                if test_ids.shape[1] > max_length:
                    # If adding this doc exceeds context,
                    # yield the current batch and start a new one
                    if not current_batch:
                        # Edge case: if a single doc is bigger than max length,
                        # we at least attempt to truncate it at the tokenizer level
                        yield [doc]
                    else:
                        # Yield the current batch (fits in context), start new
                        yield current_batch
                        current_batch = [doc]
                else:
                    # We can safely add this doc
                    current_batch = test_batch

            # Yield whatever is left
            if current_batch:
                yield current_batch

    # Convert the list of document-chunks into tokenized prompts
    prompts_and_inputs = []
    for chunk in make_chunks(data, num_documents, max_context_length):
        prompt = create_prompt(instructions, query, chunk)

        # Now tokenize with truncation just in case (to be safe).
        # If chunk_size is not None, this effectively ensures we won't exceed context.
        # If chunk_size is None, we've tried to check ourselves, but we still do a final
        # truncation pass for safety.
        inputs = tokenizer(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=max_context_length,
            padding=True
        )
        prompts_and_inputs.append((chunk, prompt, inputs))

    progress_bar = tqdm(prompts_and_inputs, desc="Inference: ")

    write_to_log(f"-----------------Inference in {model_name} model: ", log_file)

    for i, (chunk_docs, prompt_text, t) in enumerate(progress_bar):
        input_data = {k: v.cuda() for k, v in t.items()}
        model_output = model.generate(**input_data,
                                      temperature=0.8)
        decoded_text = tokenizer.batch_decode(model_output, skip_special_tokens=True)[0]

        # Create a dictionary containing the query, docs, and the answer
        cur_output_dict = {
            "query": query,
            "documents_used": chunk_docs,
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


def get_instructions(json_path, num_of_docs):
    with open(json_path, 'r') as f:
        prompts = json.load(f)

    if num_of_docs == 1:
        instruction = next(item['prompt'] for item in prompts if item['id'] == 1)
    else:
        instruction = next(item['prompt'] for item in prompts if item['id'] == 2)

    return instruction


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

    instructions = get_instructions(args.prompts, args.num_of_docs)
    model_name = args.model_name
    quantize = "27b" in model_name or "70b" in model_name
    run_model(instructions=instructions,
              data=data, query=query,
              model_name=model_name,
              log_file=output_path, num_documents=args.num_of_docs, quantize=quantize)


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
        "--prompts",
        dest="prompts",
        type=str,
        default=r"data/prompts.txt",
        help="Path to the prompts JSON file.",
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
        default=None,
        help="Number of documents to process each time.",
    )

    parser.add_argument(
        "-model_name",
        dest="model_name",
        type=str,
        default="meta-llama/Llama-3.2-3B-Instruct",
        help="HF name of LLM to run.",
    )

    args = parser.parse_args()

    main(args)
