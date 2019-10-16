import os
import shutil

folders_to_delete = [
    'giraffe_logs',
    '.nox',
    '.pytest_cache',
    'giraffe.egg-info'
]
if __name__ == '__main__':

    for root, dirs, files in os.walk(".", topdown=True):
        for folder_name in dirs:
            folder_path = os.path.join(root, folder_name)
            if folder_name in folders_to_delete:
                print(folder_path)
                shutil.rmtree(folder_path)
