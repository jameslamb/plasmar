

import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np


class DataFrameClient(object):
    """
    DataFrame client for Plasma in-mem object
    store.
    """

    def __init__(self, plasma_dir="/tmp/plasma"):

        # store some thangz
        self.plasma_dir = plasma_dir
        self.object_names = {}

        # create the client
        self.client = plasma.connect(plasma_dir, "", 0)

    def write_df(self, df, df_name):
        """
        Write a pandas dataframe to a running
        plasma store. Copied from:
        https://github.com/apache/arrow/blob/master/python/doc/source/plasma.rst
        """
        # Convert the Pandas DataFrame into a PyArrow RecordBatch
        record_batch = pa.RecordBatch.from_pandas(df)

        # Create the Plasma object from the PyArrow RecordBatch.
        # Most of the work here is done to determine the size of
        # buffer to request from the object store.
        object_id = plasma.ObjectID(np.random.bytes(20))

        # Update our little mapping from names to ObjectIDs
        self.object_names[df_name] = object_id

        # Figure out the size to allocate and create the object
        mock_sink = pa.MockOutputStream()
        stream_writer = pa.RecordBatchStreamWriter(mock_sink, record_batch.schema)
        stream_writer.write_batch(record_batch)
        stream_writer.close()
        data_size = mock_sink.size()
        print(data_size)
        buf = self.client.create(object_id, data_size)

        # Write the PyArrow RecordBatch to Plasma
        stream = pa.FixedSizeBufferWriter(buf)
        stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
        stream_writer.write_batch(record_batch)
        stream_writer.close()

        # Seal the Plasma object
        self.client.seal(object_id)

    def get_df(self, df_name):
        """
        Given some name for a dataframe, read that thing off
        of Plasma.
        """

        # Grab the plasma ObjectID
        object_id = self.object_names[df_name]

        # Fetch the Plasma object
        [data] = self.client.get_buffers([object_id])
        buffer = pa.BufferReader(data)

        # Convert object back into an Arrow RecordBatch
        reader = pa.RecordBatchStreamReader(buffer)
        record_batch = reader.read_next_batch()

        # Convert back into Pandas
        return(record_batch.to_pandas())

    def list_ids(self):
        print(self.object_names.keys())
