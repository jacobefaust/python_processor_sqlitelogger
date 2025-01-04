import argparse
from datetime import datetime
import json
import logging
from pathlib import Path
import serial
import sqlite3


class SqliteLogger:

    create_metadata_table = (
        """ CREATE TABLE metadata(
        parameter PRIMARY KEY TEXT,
        value TEXT)
        """
    )

    INSERT_INTO_METADATA = (
        "INSERT INTO metadata VALUES(?, ?)"
    )

    create_speedsamples_table = (
        """ CREATE TABLE speed_samples(
        time REAL NOT NULL, speed REAL NOT NULL)
        """
    )

    INSERT_INTO_SPEED_SAMPLES = (
        "INSERT INTO speed_samples VALUES(?, ?)"
    )

    OPS24X_CONFIG_QUERIES = {
        "??",
        "?N",
        "?D",
        "?V",
        "?B",
        "R?",
        "?F",
        "F?",
        "U?",
        "?Z",
    }    

    def __init__(self, config: dict):

        # force port
        if "PORT" not in config.keys():
            config["PORT"] = "/dev/ttyACM0"

        # force cursor buffer size
        if "CURSOR_BUFFER_SIZE" not in config.keys():
            config["CURSOR_BUFFER_SIZE"] = 10
        
        # force reporting of time
        if "OPS24X_TIME_REPORT" not in config["OPS24X_PARAMETERS"].keys():
            config["OPS24X_PARAMETERS"]["OPS24X_TIME_REPORT"] = "OT"

        self.config = config

        self.config["LOG_FILEPATH"] = (
            Path(self.config["LOG_FILEDIR"])
            / f"SpeedRecording_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        
        logging.basicConfig(
            filename=self.config["LOG_FILEPATH"], 
            level=logging.DEBUG,
            filemode='w',
            format='%(asctime)s-%(levelname)s-%(name)s:: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    def __enter__(self):
        logging.info("Entering Sqlite_Handler")
        self.serial_port = serial.Serial(
            baudrate=115200,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            bytesize=serial.EIGHTBITS,
            timeout=1,
            writeTimeout=2
        )
        self.serial_port.port = self.config["PORT"]

        self.serial_port.open()
        self.serial_port.flushInput()
        self.serial_port.flushOutput()

        self.db_conn = sqlite3.connect(
            self.config["LOG_FILEPATH"].with_suffix(".db")
        )
        self.db_cursor = self.db_conn.cursor()

        self._build_tables()

        # Initialize and query Ops24x Module
        logging.info("Initializing OPS24x Module")
        for k, v in self.config["OPS24X_PARAMETERS"]:
            self._send_ops24x_cmd(k, v)

        self._sync_time()

        logging.info("Collect OPS24X Configuration")
        for k in self.OPS24X_CONFIG_QUERIES:
            response = self._send_ops24x_cmd(
                "CONFIG_QUERY", k
            )
            response = [
                tuple(
                    y.replace('"', "")
                    for y in x.split(":")
                )
                for x in response.split("}{")
            ]
            for item in response:
                self.db_cursor.execute(
                    self.INSERT_INTO_METADATA,
                    tuple(item)
                )

    def __exit__(self):
        logging.info("Exiting Sqlite_Handler")
        self.serial_port.close()
        self.db_cursor.close()
        self.db_conn.close()

    def _build_tables(self):
        """build metadata and speed_samples tables"""
        self.db_cursor.execute(
            self.create_metadata_table
        )
        self.db_cursor.execute(
            self.create_speedsamples_table
        )

        self.db_cursor.commit()

    def _sync_time(self):
        """sync OPS24X board time with wall time"""
        self._send_ops24x_cmd(
            "OPS24X_RESET_RADAR_TIMER",
            "C=0\n"
        )
        
        self.db_cursor.execute(
            self.INSERT_INTO_METADATA,
            ("OPS24X_STARTTIME", str(datetime.now()))
        )
        self.db_cursor.commit()
        
    def _send_ops24x_cmd(
        self,
        parameter_key: str,
        ops24x_command: str
    ) -> str:
        """send commands to the OPS24x module"""
        data_for_send_str = ops24x_command
        data_for_send_bytes = str.encode(data_for_send_str)
        logging.info(f"Send {parameter_key}: {ops24x_command}")
        self.serial_port.write(data_for_send_bytes)

        data_rx_str = ""
        # Print out module response to command string
        while not ser_write_verify:
            data_rx_bytes = self.serial_port.readline()
            if len(data_rx_bytes):
                data_rx_str = str(data_rx_bytes)
                if data_rx_str.find('{'):
                    logging.debug(data_rx_str)
                    ser_write_verify = True
        return data_rx_str

    def _read_measurement(self) -> tuple:
        """read usb port for measurements"""
        measurement = None
        ops24x_rx_bytes = self.serial_port.readline()
        ops24x_rx_bytes_length = len(ops24x_rx_bytes)
        # a case can be made that if the length is 0, it's a newline char so try again
        if ops24x_rx_bytes_length:
            ops24x_rx_str = str(ops24x_rx_bytes)
            if ops24x_rx_str.find('{') == -1:  # really, { would only be found in first char
                # Speed data found (maybe)
                measurement = tuple(
                    float(x) for x in ops24x_rx_str.split(",")
                )
        return measurement

    def listen(self):
        """listen to OPS24X board for speed measurements"""

        cursor_buffer = 0

        while True:
            self.serial_port.flushInput()
            self.serial_port.flushOutput()

            meas_tup = self._read_measurement()
            if meas_tup:
                self.db_cursor.execute(
                    self.INSERT_INTO_SPEED_SAMPLES,
                    meas_tup
                )

                logging.info(
                    "Measurement: (%s, %s)" % meas_tup
                )

                cursor_buffer += 1
            
                if cursor_buffer >= self.config["CURSOR_BUFFER_SIZE"]:
                    self.db_cursor.commit()
                    cursor_buffer = 0


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("config_path",type=str, required=True)

    parser.parse_args()

    with open(Path(parser.config_path)) as r:
        config_dict = json.load(r)

    with SqliteLogger(config_dict) as sl:
        sl.listen()
# set up main process to run this async (how to make this run in the background
# without needing an open ssh session
