import time
import pandas as pd

from pathlib import Path
from docling.datamodel.base_models import InputFormat
from docling.pipeline.simple_pipeline import SimplePipeline
from docling.document_converter import DocumentConverter, PdfFormatOption, WordFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode


pipeline_options = PdfPipelineOptions( do_table_structure=True, do_ocr=True, do_cell_matching=True, use_gpu = False)
pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE  # use more accurate TableFormer model

doc_converter = DocumentConverter(
    format_options={
        InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
        InputFormat.DOCX : WordFormatOption(pipeline_cls=SimplePipeline)
    }
)

output_dir = 'docling_results'
output_dir = Path(output_dir)

start_time = time.time()

# source = "data/test.docx"  # document per local path or URL
source = "data/output_non_searchable.pdf"  # document per local path or URL

result = doc_converter.convert(source)
markdown_content = result.document.export_to_markdown()

# output_file = "alm_docling.txt"  # you can change the filename as needed
output_file = "origin_v1.txt"  # you can change the filename as needed

with open(output_file, 'w', encoding='utf-8') as f:
    f.write(markdown_content)

doc_filename = result.input.file.stem

# this was only for taking tables, i was not interested in the text itself.
# for table_idx, table in enumerate(result.document.tables):
#     table_df : pd.DataFrame = table.export_to_dataframe()

#     print(f"## Table {table_idx}")
#     print(table_df.to_markdown())

#     with open(output_file, 'a', encoding='utf-8') as f:
#         f.write(f"## Table {table_idx}")
#         f.write("\n")
#         f.write(table_df.to_markdown())
#         if not table_idx == len(result.document.tables) - 1:
#             f.write("\n\n")


end_time = time.time() - start_time
print(f"Document converted and tables exported in {end_time:.2f} seconds.")