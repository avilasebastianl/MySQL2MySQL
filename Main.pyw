import os,sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),'src'))
from utils import * # type: ignore

if __name__ == '__main__':

    the_execution.execution(sys.argv[1] if len(sys.argv) > 1 else '-h') # type: ignore
