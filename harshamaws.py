import zipfile
import os

def zip_folder_all(folder_path, output_path):
    if not os.path.exists(folder_path):
        print(f"Folder not found: {folder_path}")
        return

    files_added = 0
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)
                print(f"Added: {arcname}")
                files_added += 1

    if files_added == 0:
        print("No files found to zip.")
    else:
        print(f"ZIP file created: {output_path} ({files_added} files)")

zip_folder_all(
    r'D:\HARSHAMBOT1',
    r'D:\HARSHAMBOT1\output.zip'
)   