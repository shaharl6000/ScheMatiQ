
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
from transformers import AutoModelForCausalLM, AutoTokenizer
from tqdm import tqdm
import json
import re
import argparse
import os

access_token = "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"


class Input:
    def __init__(self, query: str, text: str, result: List):
        self.query = query
        self.text = text
        self.result = result

    def __repr__(self):
        return (f"Input(query={self.query!r}, "
                f"text={self.text!r}, "
                f"result={self.result!r})")


few_shots = [Input("Does the trip cost lots of money?",
                       "A five-day road trip for two in Tuscany features light hikes, wine tasting, and rejuvenating natural hot springs—all within a manageable drive from the city.",
                       ["Duration", "Number of Participants", "Place", "Attraction"]),
                 Input("Who will win the game?",
                       "Next Sunday, Barcelona and Real Madrid will face off in a highly anticipated soccer match that promises intense rivalry and thrilling action.",
                       ["Sport", "Time", "Team"])]

few_shots_chained = ""
for n, i in enumerate(few_shots):
    few_shots_chained += f"{n + 1}. Query: {i.query}\nText: {i.text}\nAnswer: {i.result!s}\n"


def create_prompt(cur_query, cur_data):
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


def write_to_log(message, log_file):
    print(message)
    with open(log_file, "a") as file:
        file.write(str(message) + "\n")


def run_model(data, query, log_file, model_name="meta-llama/Llama-3.2-3B-Instruct"):
    # quantization_config = BitsAndBytesConfig(load_in_4bit=True, bnb_4bit_compute_dtype=torch.bfloat16)
    model = AutoModelForCausalLM.from_pretrained(model_name, token=access_token).cuda()
    tokenizer = AutoTokenizer.from_pretrained(model_name, token=access_token)

    def get_tokenized_list(documents, cur_query):
        tokenized_list = []
        for cur_data in documents:
          tokenized_list.append(tokenizer(
              create_prompt(cur_query, cur_data),
              return_tensors="pt",
              return_attention_mask=False
          ))
        return tokenized_list

    tokenized_list = get_tokenized_list(data, query)
    progress_bar = tqdm(tokenized_list, desc="Inference: ")

    for t in progress_bar:
        input = {k: v.cuda() for k, v in t.items()}
        cur_output = tokenizer.batch_decode(model.generate(**input, max_length=350))[0]
        write_to_log("-----------------output:", log_file)
        write_to_log(cur_output, log_file)


def get_data(json_path):
    pattern = re.compile(r'text\s*=\s*"(.*?)"')
    data = []
    with open(json_path, 'r', encoding='utf-8') as f:
        for line in f:
            # Each line in your file is a separate JSON object
            json_obj = json.loads(line)

            code_snippet = json_obj.get('text', '')
            match = pattern.search(code_snippet)
            if match:
                # Group(1) is the text captured inside the quotes.
                data.append(match.group(1))
                print(match.group(1))

    return data


def main(args):
    input_path = args.input
    output_path = args.output

    name_no_ext, ext = os.path.splitext(input_path)

    metadata = {
        "domain": "",
        "dataset": name_no_ext,
        "original schema": ["Location", "Organization", "Person", "Miscellaneous"]  # correct only to CoNLL03
    }
    write_to_log("-----------------metadata:", output_path)
    write_to_log(metadata, output_path)

    query = "what is happening in the scene?"
    data = get_data(input_path)

    # data = [
    #     "Japan began the defence of their Asian Cup title with a lucky 2-1 win against Syria in a Group C championship match on Friday",
    #     "But China saw their luck desert them in the second match of the group , crashing to a surprise 2-0 defeat to newcomers Uzbekistan",
    #     "China controlled most of the match and saw several chances missed until the 78th minute when Uzbek striker Igor Shkvyrin took advantage of a misdirected defensive header to lob the ball over the advancing Chinese keeper and into an empty net .",
    #     "Oleg Shatskiku made sure of the win in injury time , hitting an unstoppable left foot shot from just outside the area .",
    #     "The former Soviet republic was playing in an Asian Cup finals tie for the first time",
    #     "Despite winning the Asian Games title two years ago , Uzbekistan are in the finals as outsiders",
    #     "Two goals from defensive errors in the last six minutes allowed Japan to come from behind and collect all three points from their opening meeting against Syria .",
    #     "At the Oval , Surrey captain Chris Lewis , another man dumped by England , continued to silence his critics as he followed his four for 45 on Thursday with 80 not out on Friday in the match against Warwickshire ."]

    run_model(data, query, output_path)



if __name__ == "__main__":
    print("start")
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i",
        "--input",
        dest="input",
        type=str,
        help="Path to the input IE data JSON file.",
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        type=str,
        help="Path where output will be saved.",
    )
    args = parser.parse_args()

    main(args)

