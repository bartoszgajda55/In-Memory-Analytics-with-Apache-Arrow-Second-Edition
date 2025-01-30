from pyarrow import fs


def main() -> None:
    f, p = fs.FileSystem.from_uri("file:///home/bartosz/projects/in_memory_arrow")
    print(f)
    print(p)
    f.create_dir("from_arrow")


if __name__ == "__main__":
    main()
