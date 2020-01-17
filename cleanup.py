import os
import shutil

from memory_profiler import profile

folders_to_delete = [
        'giraffe_logs',
        '.nox',
        '.pytest_cache',
        'Giraffe.egg-info'
]


@profile
def run():
    for root, dirs, files in os.walk(".", topdown=True):
        for folder_name in dirs:
            folder_path = os.path.join(root, folder_name)
            if folder_name in folders_to_delete:
                print(f'Cleaning {folder_path}')
                shutil.rmtree(folder_path)


if __name__ == '__main__':
    run()
