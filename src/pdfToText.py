import os
import pdfplumber

def parse_pdfs_to_text(directory: str):
    """
    Parse all PDF files in the given directory into text files.
    Each PDF will produce a .txt file with the same base name.
    """
    # Normalize and validate directory
    directory = os.path.abspath(directory)
    if not os.path.isdir(directory):
        raise NotADirectoryError(f"{directory} is not a valid directory")

    # Iterate over files
    for filename in os.listdir(directory):
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(directory, filename)
            txt_path = os.path.splitext(pdf_path)[0] + ".txt"

            print(f"Parsing: {filename} → {os.path.basename(txt_path)}")

            # Extract text
            try:
                with pdfplumber.open(pdf_path) as pdf:
                    full_text = ""
                    for page in pdf.pages:
                        text = page.extract_text() or ""
                        full_text += text + "\n"

                # Save to text file
                with open(txt_path, "w", encoding="utf-8") as f:
                    f.write(full_text)

            except Exception as e:
                print(f"❌ Failed to parse {filename}: {e}")

    print("Done!")


if __name__ == "__main__":
    print("start")
    parse_pdfs_to_text("not_related")