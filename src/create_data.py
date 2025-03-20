import PyPDF2


def text_from_data(pdf_path, output_txt_path):
    with open(pdf_path, "rb") as file:
        pdf_reader = PyPDF2.PdfReader(file)

        # Number of pages
        num_pages = len(pdf_reader.pages)

        text = ""
        for page_num in range(num_pages):
            page = pdf_reader.pages[page_num]
            text += page.extract_text()  # Extract text from each page

    # Save text to a .txt file
    with open(output_txt_path, "w", encoding="utf-8") as txt_file:
        txt_file.write(text)

    print(f"Text extracted and saved to {output_txt_path}")


if __name__ == "__main__":
    print("start")

    pdf_path = r"C:\Users\shaharl\Desktop\shahar\Uni\Information_Extraction\NES_DATA" \
               r"\pdf\Structural_prerequisites_for_CRM1.pdf"
    output_txt_path = r"C:\Users\shaharl\Desktop\shahar\Uni\Information_Extraction\NES_DATA" \
                      r"\text\Structural_prerequisites_for_CRM1.txt"
    text_from_data(pdf_path, output_txt_path)
