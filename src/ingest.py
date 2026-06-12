import os
import shutil
raw_path = "data/raw"
landing_path ="data/landing"
for file in os.listdir(raw_path):
    source_file = f"{raw_path}/{file}"
    dest_file = f"{landing_path}/{file}"
    shutil.copy (source_file, dest_file)
    print(f"copied {file}")
print("All files copied successfully")  