import os
import sys
from pathlib import Path

project_folder = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_folder))
sys.path.append(str(project_folder / "src"))
os.chdir(str(project_folder))

from zanesenie.main import main as zanesenie_run


def main() -> None:
    # subprocess.run(
    #     [r"C:\Users\robot2\Desktop\robots\damu\resources\quit_rdp.lnk"],
    #     shell=True,
    # )
    # sleep(1)
    #
    # session_name = os.environ.get("SESSIONNAME", "").lower()
    # print(f"{session_name=!r}")
    #
    # dotenv.load_dotenv()
    # server_ip = os.environ["SERVER_IP"]
    # username = os.environ["SERVER_USERNAME"]
    # password = os.environ["SERVER_PASSWORD"]
    #
    # subprocess.run(
    #     [
    #         r"C:\Users\robot2\Desktop\robots\damu\resources\sdl3-freerdp.exe",
    #         f"/v:{server_ip}",
    #         f'/u:"{username}"',
    #         f"/p:{password}",
    #         "/size:1920x1080",
    #     ],
    #     shell=True,
    # )
    #
    # for i in range(60):
    #     session_name = os.environ.get("SESSIONNAME", "").lower()
    #     print(f"{session_name=!r}")
    #     if "rdp" in session_name:
    #         break
    #     sleep(1)
    #
    # subprocess.run(["zanesenie.bat"], shell=True)

    zanesenie_run()


if __name__ == "__main__":
    main()
