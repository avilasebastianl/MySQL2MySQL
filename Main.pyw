import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'src'))
from utils import *

if __name__ == '__main__':
    TheExecution.execution(action=sys.argv[1] if len(sys.argv) > 1 else '-h')
