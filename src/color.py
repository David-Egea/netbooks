class COLOR:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def cprint(str: str, color: str) -> None:
    """ Print with color :)
        * msg: Message to print
        * color: print color COLOR CLASS COLOR
    """
    print(f"{color}{str}{COLOR.ENDC}")

def cstr(str: str, color: str) -> str:
    """ String with color
        * msg: Message to print
        * color: print color COLOR CLASS COLOR
    """
    return f"{color}{str}{COLOR.ENDC}"