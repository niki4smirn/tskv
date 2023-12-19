import os
import zipfile


def main():
    folder_path = "src/release/tmp/tskv"
    total_size = 0
    min_size = 1000000000
    max_size = 0

    for filename in os.listdir(folder_path):
        output_filename = f"{filename}.tar.gz"
        output_path = os.path.join(folder_path, output_filename)
        input_path = os.path.join(folder_path, filename)

        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zip:
            zip.write(input_path)

        current_size = os.path.getsize(output_path)
        min_size = min(min_size, current_size)
        max_size = max(max_size, current_size)
        total_size += current_size

        os.remove(output_path)

    print(f"Total size of all zipped files: {total_size} bytes")
    print(f"Min size of zipped file: {min_size} bytes")
    print(f"Max size of zipped file: {max_size} bytes")


if __name__ == "__main__":
    main()
