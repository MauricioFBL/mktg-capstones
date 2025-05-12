"""Pack all lambda functions into zip files for deployment."""

import os
import zipfile

# Define the path to your source and build directories
source_dir = "functions"
build_dir = os.path.join(source_dir, "build")
os.makedirs(build_dir, exist_ok=True)

# Each lambda function should be in its own subfolder
for fn in os.listdir(source_dir):
    fn_path = os.path.join(source_dir, fn)
    if os.path.isdir(fn_path) and fn != "build":
        zip_path = os.path.join(build_dir, f"{fn}.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(fn_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, start=fn_path)
                    zipf.write(file_path, arcname)
        print(f"Zipped {fn} -> {zip_path}")
