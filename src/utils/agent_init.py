import asyncio
import json
import platform
import sys
import os
import traceback
from shutil import copyfile
import socket
import select
import winreg
import getpass
system_paths = ['C:\\Users\\robot2\\Desktop\\robots\\damu\\src\\utils', 'C:\\Users\\robot2\\AppData\\Local\\Programs\\Python\\Python312\\python312.zip', 'C:\\Users\\robot2\\AppData\\Local\\Programs\\Python\\Python312\\DLLs', 'C:\\Users\\robot2\\AppData\\Local\\Programs\\Python\\Python312\\Lib', 'C:\\Users\\robot2\\AppData\\Local\\Programs\\Python\\Python312', 'C:\\Users\\robot2\\Desktop\\robots\\damu\\venv', 'C:\\Users\\robot2\\Desktop\\robots\\damu\\venv\\Lib\\site-packages', 'C:\\Users\\robot2\\Desktop\\robots\\damu\\venv\\Lib\\site-packages\\win32', 'C:\\Users\\robot2\\Desktop\\robots\\damu\\venv\\Lib\\site-packages\\win32\\lib', 'C:\\Users\\robot2\\Desktop\\robots\\damu\\venv\\Lib\\site-packages\\Pythonwin']

PARAMS = {}


def get_params():
    return PARAMS


def __get_params():
    try:
        data = sys.stdin.read()
        if '\\"' in data:
            data = data.strip().strip('"').replace('\\"', '"')
        params = json.loads(data)
    except:
        log_at_the_end(traceback.format_exc(), "WARNING")
        params = {}
    return params
    # else:
    #     log_at_the_end("No data available")
    #     print("No data available")
    #     return {"data": "empty"}


def print_params(params, path='params_print.txt'):
    f = open(path, "w")
    f.write(json.dumps(params))
    f.close()


def log_at_the_end(text, log_level="INFO"):
    """
    :param text:
    :param log_level: INFO/WARNING/ERROR/CRITICAL/DEBUG/EXCEPTION/NOTSET
    :return:
    """
    print(f"[{log_level}]: START_SEND_TO_ORCHESTRATOR: {text}")
    print("END_SEND_TO_ORCHESTRATOR")


def send_message_to_orchestrator(text, log_level="INFO"):
    """
    :param text:
    :param log_level: INFO/WARNING/ERROR/CRITICAL/DEBUG/EXCEPTION/NOTSET
    :return:
    """
    try:
        client.send((log_level + "||" + text + 'end_message\n').encode())
    except NameError as e:
        pass
    except Exception as e:
        log_at_the_end(e, "WARNING")
        pass


if __name__ == '__main__':
    with open(__file__) as f:
        lines = f.readlines()
    print(lines)
    for i in range(len(lines)):
        if 'system_paths =' in lines[i][0:15]:
            lines[i] = f'system_paths = {str(sys.path)}\n'
    with open(__file__, "w") as f:
        f.write("".join(lines))
    for each in system_paths:
        if each.endswith("\\site-packages"):
            try:
                copyfile(each + '\\pywin32_system32\\pythoncom37.dll', each + '\\win32\\lib\\pythoncom37.dll')
                copyfile(each + '\\pywin32_system32\\pywintypes37.dll', each + '\\win32\\lib\\pywintypes37.dll')
            except:
                pass

else:
    if not PARAMS:
        if "PythonRPA_agent" in str(os.getcwd()) or str(os.getcwd()).endswith('Core_Agent'):
            if system_paths:
                sys.path = system_paths
                try:
                    os.environ.pop("TCL_LIBRARY")
                    os.environ.pop("TK_LIBRARY")
                except:
                    pass
            try:
                PARAMS = __get_params()
                log_at_the_end(f"data received: {PARAMS}")
                PORT2 = PARAMS["socket_port_for_project"]
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.connect(('127.0.0.1', PORT2))
            except Exception as e:
                log_at_the_end(traceback.format_exc(), "WARNING")
