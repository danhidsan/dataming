
import pandas as pd
import time
import re


# Response type constants
ARRAY = 'array'
ARRAY_DICT = 'array_dict'


class StreamingSimulator:

    def __init__(self, file_path, lapse=1, data_window=1, response_type=ARRAY):
        self.file_path = file_path
        self.lapse = lapse
        self.data_window = data_window
        self.response_type = self.__check_response_type(response_type)

    @staticmethod
    def __check_response_type(response_type):
        if response_type == 'array' or response_type == 'array_dict':
            return response_type
        else:
            raise Exception("Response type not supported")


class StreamingSimulatorCsv(StreamingSimulator):

    def __init__(self, file_path, lapse=1, data_window=1, response_type=ARRAY, sep=',',
                    delimiter=None, delim_whitespace=False):

        # Parent class
        StreamingSimulator.__init__(self, file_path, lapse, data_window, response_type)

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
            csv = pd.read_csv(
                self.file_path,
                sep=self.sep,
                delimiter=self.delimiter,
                delim_whitespace=self.delim_whitespace)

            return csv
        except Exception as e:
            print(e)

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

        window = self.data_window
        if self.response_type == ARRAY:
            rows_values = self.__data.values

            for index in range(0, len(rows_values), window):
                time.sleep(self.lapse)
                on_simulate(rows_values[index:index+window])
        elif self.response_type == ARRAY_DICT:
            rows = self.__data.to_dict(orient='records')
            for index in range(0, len(rows), window):
                time.sleep(self.lapse)
                on_simulate(rows[index:index+window])


class StreamingSimulatorJSON(StreamingSimulator):

    def __init__(self, file_path, lapse=1, data_window=1, response_type=ARRAY, lines=False):

        # Parent class
        StreamingSimulator.__init__(self, file_path, lapse, data_window, response_type)

        # Checks
        self.check_json(file_path)

        # Attributes
        self.lines = lines
        self.__data = self.__read_file

    @staticmethod
    def check_json(file_path):
        """Check if file from file path, is JSON file

               Params:
                   file_path -- File path string

               Exceptions:
                   Exception -- If file from file path is not JSON file
               """
        search = re.search('.json', file_path)

        if search is None:
            raise Exception("File path must be JSON file")

    @property
    def __read_file(self):

        try:
            json = pd.read_json(
                self.file_path,
                typ='series',
                lines=self.lines,
                orient='records')
            return json
        except ValueError:
            raise ValueError("JSON Format not correct")
        except Exception as e:
            raise e

    def simulate(self, on_simulate=print):
        """Simulate streaming data flow.

                Params:

                    on_simulate -- function callback to define row by row action
        """
        shapes = self.__data.shape
        print("Simulating %d rows..." % (shapes[0],))

        rows_values = self.__data
        len_data = len(rows_values)
        window = self.data_window

        for index in range(0, len_data, window):
            time.sleep(self.lapse)
            on_simulate(rows_values[index:index+window].values)
