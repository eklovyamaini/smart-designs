import datetime
from zoneinfo import ZoneInfo
import os
import logging
import json


class FileSystemTools:
    """A class to provide simple, workspace-aware file system tools."""
    def __init__(self, root_dir: str):
        self.root_dir = root_dir
        logging.info(f"FileSystemTools initialized with root_dir: {self.root_dir}")

    def _get_full_path(self, relative_path: str) -> str:
        """Constructs the full, absolute path safely."""
        return os.path.join(self.root_dir, relative_path)

    def write_file(self, path: str, content: str) -> dict:
        """Writes content to a file at the specified relative path."""
        logging.debug(f"Attempting to write file at path: {path}")
        full_path = self._get_full_path(path)
        try:
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, 'w') as f:
                f.write(content)
            logging.info(f"Successfully wrote file to {path}")
            return {"status": "success", "report": f"File written successfully to {path}"}
        except Exception as e:
            logging.error(f"Error writing file to {path}: {e}")
            return {"status": "error", "error_message": str(e)}

    def read_file(self, path: str) -> dict:
        """Reads the content of a file at the specified relative path."""
        logging.debug(f"Attempting to read file from path: {path}")
        full_path = self._get_full_path(path)
        try:
            with open(full_path, 'r') as f:
                content = f.read()
            logging.info(f"Successfully read file from {path}")
            return {"status": "success", "content": content}
        except Exception as e:
            logging.error(f"Error reading file from {path}: {e}")
            return {"status": "error", "error_message": str(e)}

    def list_directory(self, path: str) -> dict:
        """Lists the contents of a directory at the specified relative path."""
        logging.debug(f"Attempting to list directory at path: {path}")
        full_path = self._get_full_path(path)
        try:
            contents = os.listdir(full_path)
            logging.info(f"Successfully listed directory {path}")
            return {"status": "success", "contents": contents}
        except Exception as e:
            logging.error(f"Error listing directory {path}: {e}")
            return {"status": "error", "error_message": str(e)}

    def log_instruction(instruction: str) -> dict:
        """Logs the agent's instructions to the debug log."""
        logging.info("---AGENT INSTRUCTION---")
        logging.info(instruction)
        logging.info("--------------------")
        return {"status": "success", "report": "Instruction logged."}
