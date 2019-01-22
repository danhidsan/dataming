
import pandas as pd
import time
import sys
import re


class StreamingSimulator:

    def __init__(self, file_path, lapse=1, data_window=1):
        self.file_path = file_path
        self.lapse = lapse
        self.data_window = data_window


class StreamingSimulatorCsv(StreamingSimulator):

    def __init__(self, file_path, lapse=1, data_window=1, sep=',', delimiter=None, delim_whitespace=False):

        # Parent class
        StreamingSimulator.__init__(self, file_path, lapse, data_window)

        # Checks
        self.check_csv(file_path)

        # Attributes
        self.sep = sep
        self.delimiter = delimiter
        self.delim_whitespace = delim_whitespace
        self.__data = self.__read_file

    @staticmethod
    def check_csv(file_path):
        """Check if file from file path, is CSV file

        Params:
            file_path -- File path string

        Exceptions:
            Exception -- If file from file path is not CSV file
        """
        search = re.search('.csv', file_path)

        if search is None:
            raise Exception("File path must be CSV file")

    @property
    def __read_file(self):

        try:
            readed_csv = pd.read_csv(
                self.file_path,
                sep=self.sep,
                delimiter=self.delimiter,
                delim_whitespace=self.delim_whitespace)

            return readed_csv
        except:
            print("Unexpected error:", sys.exc_info()[0])

    def data_head(self, lines=5):
        """Return head data from params lines

        Params:
            lines -- lines of head (default: 5)
        """
        return self.__data.head(lines)

    def simulate(self, on_simulate=print):
        """Simulate streaming data flow.

        Params:

            on_simulate -- function callback to define row by row action
        """

        shapes = self.__data.shape
        print("Simulating %d rows..." % (shapes[0], ))

        rows_values = self.__data.values
        len_data = len(rows_values)
        window = self.data_window

        for index in range(0, len_data, window):
            time.sleep(self.lapse)
            on_simulate(rows_values[index:index+window])


