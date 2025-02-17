import pyarrow as pa
from pyarrow import fs
import pyarrow.parquet as pq
import pyarrow.flight as flight


class Server(flight.FlightServerBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._s3 = fs.S3FileSystem(region="us-east-2", anonymous=True)

    def list_flights(self, context, criteria: bytes):
        path = "ursa-labs-taxi-data"
        if len(criteria) > 0:
            path += "/" + criteria.decode("utf8")
        flist = self._s3.get_file_info(fs.FileSelector(path, recursive=True))
        for finfo in flist:
            if finfo.type == fs.FileType.Directory:
                continue
            with self._s3.open_input_file(finfo.path) as f:
                data = pq.ParquetFile(f)
                yield flight.FlightInfo(
                    data.schema_arrow,
                    flight.FlightDescriptor.for_path(finfo.path),
                    [],
                    data.metadata.num_rows,
                    -1,
                )

    def do_get(self, context, ticket):
        input = self._s3.open_input_file(ticket.ticket.decode("utf8"))
        pf = pq.ParquetFile(input, pre_buffer=True)

        def gen():
            try:
                for batch in pf.iter_batches():
                    yield batch
            finally:
                input.close()  # make sure we always close the file

        return flight.GeneratorStream(pf.schema_arrow, gen())


if __name__ == "__main__":
    with Server() as server:
        client = flight.connect(("localhost", server.port))
        for f in client.list_flights(b"2009"):
            print(f"Path: {f.descriptor.path}, Total Records: {f.total_records}")
