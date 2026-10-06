import asyncio
import os
import sys
from nbconvert import WebPDFExporter
from nbconvert.writers import FilesWriter

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

notebook_name = "eda1.ipynb"

exporter = WebPDFExporter()
exporter.allow_chromium_download = True

writer = FilesWriter(build_directory=".")

body, resources = exporter.from_filename(notebook_name)
writer.write(body, resources, notebook_name=notebook_name)

print("PDF export finished.")