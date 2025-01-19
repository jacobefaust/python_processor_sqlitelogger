import argparse
from datetime import datetime
import json
import logging
from pathlib import Path
import serial
import sqlite3
import sys
import time
from typing import List, Tuple


class SqliteLogger:

    create_metadata_table = (
        """ CREATE TABLE metadata(
        parameter TEXT PRIMARY KEY NOT NULL,
        value TEXT)
        """
    )

    INSERT_INTO_METADATA = (
        "INSERT INTO metadata VALUES(?, ?)"
    )

    create_speedsamples_table = (
        """ CREATE TABLE speed_samples(
        time REAL PRIMARY KEY NOT NULL,
        speed REAL NOT NULL)
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

        self.config = config

        self.config["LOG_FILEPATH"] = (
            Path(self.config["LOG_FILEDIR"])
            / f"SpeedRecording_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        
        logging.basicConfig(
            format='%(asctime)s-%(levelname)s-%(name)s:: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            level=logging.INFO,
            handlers=[
                logging.FileHandler(
                    self.config["LOG_FILEPATH"],
                    mode="w"
                ),
                logging.StreamHandler(stream=sys.stdout)
            ]
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
        logging.info('Serial Port Connection Established')

        self.db_conn = sqlite3.connect(
            self.config["LOG_FILEPATH"].with_suffix(".db")
        )
        self.db_cursor = self.db_conn.cursor()
        logging.info('Sqlite Database Connection Established')
        self._build_tables()
        logging.info("Sqlite Database Tables Constructed")

        # Initialize and query Ops24x Module
        logging.info("Initializing OPS24x Module")
        for v in self.config["OPS24X_PARAMETERS"].values():
            self._send_ops24x_cmd(v)

        # force time reporting
        self._send_ops24x_cmd("OT")

        # handle out-of-range sampling
        self._send_ops24x_cmd("BT")

        self._sync_time()

        logging.info("Collect OPS24X Configuration")
        for k in self.OPS24X_CONFIG_QUERIES:
            response = self._send_ops24x_cmd(
                "CONFIG_QUERY", k
            )
            for item in response:
                self.db_cursor.execute(
                    self.INSERT_INTO_METADATA,
                    tuple(item)
                )
            
            self.db_conn.commit()

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

        self.db_conn.commit()

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
        self.db_conn.commit()
        
    def _send_ops24x_cmd(
        self,
        ops24x_command: str
    ) -> List[Tuple]:
        """send commands to the OPS24x module and returns
        back a list of tuples containing key, value pairs
        of various configuration settings

        PARAMETERS
        ----------
        ops24x_command:  the two letter coded configuration
        query

        RETURNS
        -------
        Utilizing the non-json output format, tuples of
        (parameter, value) of the OPS24X board is returned.
        
        """
        logging.info(f"Send: {ops24x_command}")
        self.serial_port.write(
            str.encode(ops24x_command)
        )

        # Print out module response to command string
        data_rx_list = self.serial_port.readlines()
        out_list = []
        for sample_bytes in data_rx_list:
            sample_str = sample_bytes.decode()

            if "{" in sample_str:
                sample_str = (
                    sample_str[1:-3].replace('"', '')
                )
                for entry in sample_str.split(","):
                    out_list.append(tuple(entry.split(":")))

        return out_list

    def _read_measurement(self) -> tuple:
        """read usb port for measurements
        
        RETURNS
        -------
        None if no measurement detected (the enforced
        output settings ensure just a timestamp is sent)

        Otherwise a tuple of (sample time, measured speed)
        is sent where each value is a float.
        """
        measurement = None
        ops24x_rx_bytes = self.serial_port.readline()
        ops24x_rx_str = ops24x_rx_bytes.decode()
        if ops24x_rx_str.find(',') == -1:
            measurement = tuple(
                float(x) for x in ops24x_rx_str[:-2].split(",")
            )
        return measurement

    def listen(self):
        """listen to OPS24X board for speed measurements"""
        logging.info("Entering Measurement Listening Mode")

        cursor_buffer = 0

        start_time = time.time()
        while True:
            self.serial_port.flushInput()
            self.serial_port.flushOutput()

            if (
                "MAX_TIME" in self.config
                and time.time() - start_time > self.config["MAX_TIME"]
            ):
                break

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
                    self.db_conn.commit()
                    cursor_buffer = 0


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("config_path", type=str)

    args = parser.parse_args()

    with open(Path(args.config_path), 'r') as r:
        config_dict = json.load(r)

    with SqliteLogger(config_dict) as sl:

        sl.listen()
