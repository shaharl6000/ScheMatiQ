import argparse
import ast
import re
import matplotlib
matplotlib.use('Agg')  # Use a backend that doesn't require a display (for servers)
import matplotlib.pyplot as plt


def main(args):
    # Regex to find every bracketed array of the fields
    answer_pattern = re.compile(r"Answer:\s*(\[[^\]]*\])")  # TODO will need to change to "Fields"

    field_counts = {}
    invalid_lines_count = 0

    with open(args.input, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                data_dict = ast.literal_eval(line)
            except:
                continue

            answer_text = data_dict.get("answer", "")
            if not answer_text:
                continue

            matches = answer_pattern.findall(answer_text)
            if not matches:
                # No array found
                invalid_lines_count += 1
                continue

            # We only want the LAST occurrence (to skip any few-shot examples)
            array_str = matches[-1]

            # Safely parse the bracketed array (e.g., ['Rank', 'Country']) into a Python list
            try:
                fields = ast.literal_eval(array_str)
            except:
                invalid_lines_count += 1
                continue

            # Check that we got a list
            if not isinstance(fields, list):
                invalid_lines_count += 1
                continue

            # Increment counts for each field in the list
            for field in fields:
                field_counts[field] = field_counts.get(field, 0) + 1

    # Summary of invalid lines
    print(f"Number of lines without a valid array: {invalid_lines_count}")

    # Sort fields by their frequency (descending)
    sorted_fields = sorted(field_counts.items(), key=lambda x: x[1], reverse=True)
    # TODO: ideas -
    #  filter by only what was in at least x percentage of the data
    #  merge ones that have similar meanings (with an llm..)

    # Print sorted field counts
    print("Field counts (sorted by frequency):")
    for field, count in sorted_fields:
        print(f"{field}: {count}")

    if not sorted_fields:
        print("No valid fields to plot.")
        return

    # Plot as a bar chart
    fields, counts = zip(*sorted_fields)  # Unzip into two lists
    plt.bar(fields, counts)
    plt.xlabel("Fields")
    plt.ylabel("Frequency")
    plt.title("Histogram of Fields Extracted from LLM Answers")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig("histogram.png")
    print("Histogram saved as histogram.png")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input",
        dest="input",
        type=str,
        required=True,
        help="Path to the input .txt file.",
    )
    args = parser.parse_args()
    main(args)
