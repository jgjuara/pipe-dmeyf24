import subprocess
import sys

def install_requirements(requirements_file):
    with open(requirements_file, 'r') as file:
        for line in file:
            package = line.strip()
            if package:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Specify the path to your requirements.txt file
requirements_file = 'requirements.txt'
install_requirements(requirements_file)